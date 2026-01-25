.PHONY: run run-dump chat build

CHAT_PORT ?= 8080
API_ADDR ?= 127.0.0.1:3000

run: build
	CLAUDE_SERVER_LISTEN=$(API_ADDR) ./target/debug/claude-server

run-dump: build
	CLAUDE_SERVER_LISTEN=$(API_ADDR) ./target/debug/claude-server --dump-turns

chat: build
	@echo "Opening http://127.0.0.1:$(CHAT_PORT) ..."
	@open "http://127.0.0.1:$(CHAT_PORT)" 2>/dev/null || true
	./target/debug/claude-server chat --port $(CHAT_PORT) --api-url http://$(API_ADDR)

build:
	cargo build
