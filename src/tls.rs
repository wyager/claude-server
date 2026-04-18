//! HTTPS + ACME support for the chat UI.
//!
//! Three modes, selected by `TlsArgs`:
//! 1. Plain HTTP (neither `--tls-cert` nor `--acme-domain`) — caller runs the
//!    non-TLS axum::serve path.
//! 2. Static PEM files (`--tls-cert` + `--tls-key`) — suits cert-manager or
//!    any workflow that drops certs into a known path.
//! 3. Let's Encrypt (`--acme-domain` + `--acme-email`) with either HTTP-01 or
//!    DNS-01 verification. DNS-01 uses a user-provided hook script so any
//!    DNS provider can plug in without bundling SDKs.
//!
//! Renewal runs in a background task scheduled deadline-driven off the cert's
//! NotAfter minus 30 days — no polling.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use axum::routing::get;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use clap::{Args, ValueEnum};
use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, ChallengeType, Identifier, LetsEncrypt,
    NewAccount, NewOrder, OrderStatus, RetryPolicy,
};
use tokio::sync::RwLock;
use x509_parser::pem::parse_x509_pem;

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum AcmeMethod {
    /// HTTP-01: serve challenge tokens on `--acme-http-bind` (port 80 by default).
    #[value(name = "http-01")]
    Http01,
    /// DNS-01: invoke `--acme-dns-hook` to manage TXT records.
    #[value(name = "dns-01")]
    Dns01,
}

#[derive(Clone, Debug, Args)]
pub struct TlsArgs {
    /// Path to TLS certificate (PEM). With --tls-key, enables HTTPS using
    /// static files (suits cert-manager or manually-provisioned certs).
    #[arg(long, value_name = "PATH")]
    pub tls_cert: Option<PathBuf>,

    /// Path to TLS private key (PEM). Requires --tls-cert.
    #[arg(long, value_name = "PATH", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    /// Obtain a Let's Encrypt certificate for this domain. Repeat for SAN.
    /// Mutually exclusive with --tls-cert.
    #[arg(long, value_name = "DOMAIN")]
    pub acme_domain: Vec<String>,

    /// Contact email for the Let's Encrypt account. Required with --acme-domain.
    #[arg(long, value_name = "EMAIL", requires = "acme_domain")]
    pub acme_email: Option<String>,

    /// ACME verification method.
    #[arg(long, value_enum, default_value_t = AcmeMethod::Http01)]
    pub acme_method: AcmeMethod,

    /// Use the Let's Encrypt staging environment (for testing without rate limits).
    #[arg(long)]
    pub acme_staging: bool,

    /// Where to persist the ACME account credentials and issued certificate.
    #[arg(long, value_name = "PATH", default_value = "./acme-cache")]
    pub acme_cache_dir: PathBuf,

    /// Bind address for the HTTP-01 challenge responder. Let's Encrypt must be
    /// able to reach your domain on this address (typically port 80).
    #[arg(long, value_name = "ADDR", default_value = "0.0.0.0:80")]
    pub acme_http_bind: SocketAddr,

    /// External script for DNS-01 challenges. Invoked as:
    ///   <hook> add <fqdn> <value>
    ///   <hook> remove <fqdn> <value>
    /// The hook must return only after the record is live at the authoritative
    /// nameservers (poll/verify internally). Required for --acme-method=dns-01.
    #[arg(long, value_name = "PATH")]
    pub acme_dns_hook: Option<PathBuf>,
}

impl TlsArgs {
    pub fn is_enabled(&self) -> bool {
        self.tls_cert.is_some() || !self.acme_domain.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        if self.tls_cert.is_some() && !self.acme_domain.is_empty() {
            bail!("--tls-cert and --acme-domain are mutually exclusive");
        }
        if self.tls_cert.is_some() && self.tls_key.is_none() {
            bail!("--tls-key is required with --tls-cert");
        }
        if !self.acme_domain.is_empty() && self.acme_email.is_none() {
            bail!("--acme-email is required when --acme-domain is set");
        }
        if self.acme_method == AcmeMethod::Dns01
            && !self.acme_domain.is_empty()
            && self.acme_dns_hook.is_none()
        {
            bail!("--acme-dns-hook is required when --acme-method=dns-01");
        }
        Ok(())
    }
}

pub async fn serve(addr: SocketAddr, app: Router, tls: TlsArgs) -> Result<()> {
    // rustls 0.23 with the no-provider feature requires an explicit install.
    // install_default returns Err if one is already installed — harmless here.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let material = if tls.tls_cert.is_some() {
        load_static(&tls)?
    } else {
        load_or_obtain_acme(&tls).await?
    };

    let config = RustlsConfig::from_pem(material.cert_pem.clone(), material.key_pem.clone())
        .await
        .context("build rustls config")?;

    if !tls.acme_domain.is_empty() {
        let tls_clone = tls.clone();
        let config_clone = config.clone();
        let initial_expiry = material.not_after;
        tokio::spawn(async move {
            if let Err(e) = renewal_loop(tls_clone, config_clone, initial_expiry).await {
                eprintln!("[acme] renewal loop exited: {:#}", e);
            }
        });
    }

    println!("HTTPS listening on https://{}", addr);
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .context("axum-server serve")
}

struct CertMaterial {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    not_after: SystemTime,
}

fn load_static(tls: &TlsArgs) -> Result<CertMaterial> {
    let cert_path = tls.tls_cert.as_ref().expect("validated");
    let key_path = tls
        .tls_key
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("--tls-key is required with --tls-cert"))?;
    let cert_pem = std::fs::read(cert_path)
        .with_context(|| format!("read {}", cert_path.display()))?;
    let key_pem = std::fs::read(key_path)
        .with_context(|| format!("read {}", key_path.display()))?;
    let not_after = cert_not_after(&cert_pem)?;
    Ok(CertMaterial { cert_pem, key_pem, not_after })
}

async fn load_or_obtain_acme(tls: &TlsArgs) -> Result<CertMaterial> {
    tokio::fs::create_dir_all(&tls.acme_cache_dir)
        .await
        .with_context(|| format!("create ACME cache dir {}", tls.acme_cache_dir.display()))?;

    let cert_path = tls.acme_cache_dir.join("cert.pem");
    let key_path = tls.acme_cache_dir.join("key.pem");
    if cert_path.exists() && key_path.exists() {
        let cert_pem = tokio::fs::read(&cert_path).await?;
        let key_pem = tokio::fs::read(&key_path).await?;
        match cert_not_after(&cert_pem) {
            Ok(not_after) => {
                let remaining = not_after
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO);
                if remaining > Duration::from_secs(30 * 86400) {
                    println!(
                        "[acme] reusing cached cert ({} days remaining)",
                        remaining.as_secs() / 86400
                    );
                    return Ok(CertMaterial { cert_pem, key_pem, not_after });
                }
                println!(
                    "[acme] cached cert has {} days left — renewing now",
                    remaining.as_secs() / 86400
                );
            }
            Err(e) => eprintln!("[acme] cached cert unreadable ({}) — reissuing", e),
        }
    }

    issue_cert(tls).await
}

async fn issue_cert(tls: &TlsArgs) -> Result<CertMaterial> {
    let http_responder = if tls.acme_method == AcmeMethod::Http01 {
        Some(Http01Responder::start(tls.acme_http_bind).await?)
    } else {
        None
    };
    let mut dns_cleanups: Vec<(String, String)> = Vec::new();

    let result = issue_cert_inner(tls, http_responder.as_ref(), &mut dns_cleanups).await;

    // Clean up DNS records regardless of success/failure.
    if let Some(hook) = &tls.acme_dns_hook {
        for (fqdn, value) in &dns_cleanups {
            if let Err(e) = run_dns_hook(hook, "remove", fqdn, value).await {
                eprintln!("[acme] dns cleanup for {}: {}", fqdn, e);
            }
        }
    }
    // http_responder drops here, stopping the challenge listener.

    result
}

async fn issue_cert_inner(
    tls: &TlsArgs,
    http_responder: Option<&Http01Responder>,
    dns_cleanups: &mut Vec<(String, String)>,
) -> Result<CertMaterial> {
    let account = load_or_create_account(tls).await?;

    let identifiers: Vec<Identifier> = tls
        .acme_domain
        .iter()
        .map(|d| Identifier::Dns(d.clone()))
        .collect();

    let mut order = account
        .new_order(&NewOrder::new(&identifiers))
        .await
        .context("create ACME order")?;

    let challenge_type = match tls.acme_method {
        AcmeMethod::Http01 => ChallengeType::Http01,
        AcmeMethod::Dns01 => ChallengeType::Dns01,
    };

    let mut auths = order.authorizations();
    while let Some(result) = auths.next().await {
        let mut authz = result.context("fetch authorization")?;
        match authz.status {
            AuthorizationStatus::Pending => {}
            AuthorizationStatus::Valid => continue,
            other => bail!("unexpected authorization status: {:?}", other),
        }

        let mut challenge = authz.challenge(challenge_type.clone()).ok_or_else(|| {
            anyhow::anyhow!("ACME server offered no {:?} challenge", challenge_type)
        })?;
        let domain = challenge.identifier().to_string();
        let ka = challenge.key_authorization();

        match tls.acme_method {
            AcmeMethod::Http01 => {
                let responder = http_responder.expect("http-01 responder missing");
                responder
                    .insert(challenge.token.clone(), ka.as_str().to_string())
                    .await;
            }
            AcmeMethod::Dns01 => {
                let fqdn = format!("_acme-challenge.{}", strip_wildcard(&domain));
                let value = ka.dns_value();
                let hook = tls
                    .acme_dns_hook
                    .as_ref()
                    .expect("dns-01 hook missing (validated)");
                run_dns_hook(hook, "add", &fqdn, &value)
                    .await
                    .context("dns-01 hook (add)")?;
                dns_cleanups.push((fqdn, value));
            }
        }

        challenge
            .set_ready()
            .await
            .context("notify ACME that challenge is ready")?;
    }
    drop(auths);

    let status = order
        .poll_ready(&RetryPolicy::default())
        .await
        .context("poll order ready")?;
    if status != OrderStatus::Ready {
        bail!("ACME order did not reach Ready: {:?}", status);
    }

    let private_key_pem = order.finalize().await.context("finalize order")?;
    let cert_chain_pem = order
        .poll_certificate(&RetryPolicy::default())
        .await
        .context("poll for certificate")?;

    let cert_pem_bytes = cert_chain_pem.into_bytes();
    let key_pem_bytes = private_key_pem.into_bytes();
    tokio::fs::write(tls.acme_cache_dir.join("cert.pem"), &cert_pem_bytes).await?;
    tokio::fs::write(tls.acme_cache_dir.join("key.pem"), &key_pem_bytes).await?;
    let not_after = cert_not_after(&cert_pem_bytes)?;
    println!(
        "[acme] issued cert for {} (expires {})",
        tls.acme_domain.join(", "),
        format_time(not_after)
    );

    Ok(CertMaterial { cert_pem: cert_pem_bytes, key_pem: key_pem_bytes, not_after })
}

fn strip_wildcard(s: &str) -> &str {
    s.strip_prefix("*.").unwrap_or(s)
}

async fn load_or_create_account(tls: &TlsArgs) -> Result<Account> {
    let creds_path = tls.acme_cache_dir.join("account.json");
    let directory_url = if tls.acme_staging {
        LetsEncrypt::Staging.url().to_owned()
    } else {
        LetsEncrypt::Production.url().to_owned()
    };

    if creds_path.exists() {
        let bytes = tokio::fs::read(&creds_path).await?;
        let credentials: AccountCredentials = serde_json::from_slice(&bytes)
            .context("parse cached ACME account credentials")?;
        let account = Account::builder()
            .context("ACME client")?
            .from_credentials(credentials)
            .await
            .context("restore ACME account")?;
        return Ok(account);
    }

    let email = tls.acme_email.as_ref().expect("validated");
    let contact = format!("mailto:{}", email);
    let contacts = [contact.as_str()];
    let (account, credentials) = Account::builder()
        .context("ACME client")?
        .create(
            &NewAccount {
                contact: &contacts,
                terms_of_service_agreed: true,
                only_return_existing: false,
            },
            directory_url,
            None,
        )
        .await
        .context("create ACME account")?;

    let json = serde_json::to_vec_pretty(&credentials)?;
    tokio::fs::write(&creds_path, json).await?;
    println!(
        "[acme] created new account ({})",
        if tls.acme_staging { "staging" } else { "production" }
    );
    Ok(account)
}

type ChallengeMap = std::collections::HashMap<String, String>;

struct Http01Responder {
    tokens: Arc<RwLock<ChallengeMap>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Http01Responder {
    async fn start(bind: SocketAddr) -> Result<Self> {
        let tokens: Arc<RwLock<ChallengeMap>> = Arc::new(RwLock::new(ChallengeMap::new()));
        let tokens_for_router = tokens.clone();
        let app = Router::new().route(
            "/.well-known/acme-challenge/{token}",
            get(move |axum::extract::Path(token): axum::extract::Path<String>| {
                let tokens = tokens_for_router.clone();
                async move {
                    let map = tokens.read().await;
                    match map.get(&token) {
                        Some(v) => (axum::http::StatusCode::OK, v.clone()),
                        None => (
                            axum::http::StatusCode::NOT_FOUND,
                            "not found".to_string(),
                        ),
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .with_context(|| format!("bind HTTP-01 responder on {}", bind))?;
        println!("[acme] HTTP-01 responder listening on http://{}", bind);

        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await;
        });

        Ok(Self { tokens, shutdown: Some(tx), task: Some(task) })
    }

    async fn insert(&self, token: String, value: String) {
        self.tokens.write().await.insert(token, value);
    }
}

impl Drop for Http01Responder {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_dns_hook(hook: &Path, action: &str, fqdn: &str, value: &str) -> Result<()> {
    let output = tokio::process::Command::new(hook)
        .arg(action)
        .arg(fqdn)
        .arg(value)
        .output()
        .await
        .with_context(|| format!("spawn dns-01 hook {}", hook.display()))?;
    if !output.status.success() {
        bail!(
            "dns-01 hook {} {} {} failed (exit {:?}): stdout={} stderr={}",
            hook.display(),
            action,
            fqdn,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(())
}

async fn renewal_loop(
    tls: TlsArgs,
    config: RustlsConfig,
    mut not_after: SystemTime,
) -> Result<()> {
    loop {
        let renew_at = not_after
            .checked_sub(Duration::from_secs(30 * 86400))
            .unwrap_or_else(SystemTime::now);
        let wait = renew_at
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::from_secs(60));
        println!(
            "[acme] next renewal attempt in ~{}h ({} days before expiry)",
            wait.as_secs() / 3600,
            30
        );
        tokio::time::sleep(wait).await;

        match issue_cert(&tls).await {
            Ok(new_material) => {
                if let Err(e) = config
                    .reload_from_pem(new_material.cert_pem.clone(), new_material.key_pem.clone())
                    .await
                {
                    eprintln!("[acme] renewal: reload_from_pem failed: {}", e);
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    continue;
                }
                not_after = new_material.not_after;
                println!("[acme] cert renewed, new expiry {}", format_time(not_after));
            }
            Err(e) => {
                eprintln!("[acme] renewal failed: {:#} — retrying in 1h", e);
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }
}

fn cert_not_after(pem_bytes: &[u8]) -> Result<SystemTime> {
    let (_, pem) = parse_x509_pem(pem_bytes).map_err(|e| anyhow::anyhow!("parse PEM: {}", e))?;
    let cert = pem
        .parse_x509()
        .map_err(|e| anyhow::anyhow!("parse X.509: {}", e))?;
    let ts = cert.validity().not_after.timestamp();
    if ts < 0 {
        bail!("cert not_after is before 1970");
    }
    Ok(UNIX_EPOCH + Duration::from_secs(ts as u64))
}

fn format_time(t: SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    dt.format("%Y-%m-%d %H:%M:%SZ").to_string()
}
