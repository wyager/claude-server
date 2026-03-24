#!/usr/bin/env python3
"""
Diff consecutive API request JSON dumps to find cache-breaking divergence.
Usage: diff-requests.py /var/log/claude-json root
"""
import sys, json, hashlib, glob

def h(s): return hashlib.sha256(s.encode() if isinstance(s,str) else s).hexdigest()[:12]

d, agent = sys.argv[1], sys.argv[2] if len(sys.argv)>2 else 'root'
files = sorted(glob.glob(f"{d}/{agent}-*.json"))
prev = None
for f in files:
    req = json.load(open(f))
    sys_txt = req['system'][0]['text']
    tools = json.dumps(req['tools'], sort_keys=True)
    blocks = req['messages'][0]['content']
    sig = [f"sys:{h(sys_txt)}:{len(sys_txt)}", f"tools:{h(tools)}"]
    for i,b in enumerate(blocks):
        if b['type']=='text':
            cc = '+cc' if b.get('cache_control') else ''
            sig.append(f"txt[{i}]{cc}:{h(b['text'])}:{len(b['text'])}")
        elif b['type']=='image':
            cc = '+cc' if b.get('cache_control') else ''
            sig.append(f"img[{i}]{cc}:{h(b['source']['data'])}")
    name = f.split('/')[-1]
    print(f"{name}: {' '.join(sig)}")
    if prev:
        psys, ptools, *pblocks = prev
        if psys != sig[0]: print(f"  ! SYSTEM changed")
        if ptools != sig[1]: print(f"  ! TOOLS changed")
        for i,(a,b) in enumerate(zip(pblocks, sig[2:])):
            if a.split(':')[0] != b.split(':')[0]:
                print(f"  ! block[{i}] type/cc changed: {a} -> {b}")
            elif a != b:
                # same type, diff content — check if text block is prefix-ext
                if 'txt' in a:
                    pt = json.load(open(files[files.index(f)-1]))['messages'][0]['content'][i]['text']
                    ct = blocks[i]['text']
                    if ct.startswith(pt):
                        print(f"  ✓ block[{i}] prefix-ext (+{len(ct)-len(pt)} chars)")
                    else:
                        for j in range(min(len(pt),len(ct))):
                            if pt[j]!=ct[j]:
                                print(f"  ! block[{i}] DIVERGE@{j}: ...{repr(pt[j-40:j+40])}")
                                print(f"                        vs  ...{repr(ct[j-40:j+40])}")
                                break
    prev = sig
