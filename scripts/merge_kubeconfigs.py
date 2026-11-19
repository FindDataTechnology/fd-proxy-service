#!/usr/bin/env python3
"""Merge worker kubeconfig keys (token+ca) into the control-plane
worker-kubeconfigs Secret (run on guangzhou-the db host as ubuntu).

Usage (on the db host, with staged files at /tmp/<cluster>.{token,ca}):
    python3 scripts/merge_kubeconfigs.py the node america

Reads the existing fd-master/worker-kubeconfigs Secret, PRESERVES every
existing key (tencent/aliyun/baidu/...), base64-encodes the staged files,
merges the new keys in, and kubectl-apply-s the result via a 0600 temp
manifest that is unlinked immediately after. Secrets never appear in argv,
ps, or stdout — only MERGE_OK/KEYS lines. Token files are rstrip()ped of
trailing newlines before encoding (a stray \\n breaks Bearer auth in
ClusterK8sClient); .ca bytes are kept verbatim.
"""
import base64
import json
import os
import subprocess
import sys

NS, NAME = "fd-master", "worker-kubeconfigs"


def kubectl(*args):
    r = subprocess.run(["sudo", "k3s", "kubectl", *args],
                       capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit("KCTL_FAIL: %s" % r.stderr.strip())
    return r.stdout


def main():
    clusters = sys.argv[1:]
    if not clusters:
        sys.exit("usage: merge_kubeconfigs.py <cluster> [cluster ...]")
    cur = json.loads(kubectl("-n", NS, "get", "secret", NAME, "-o", "json"))
    data = dict(cur.get("data") or {})
    for c in clusters:
        for ext in ("token", "ca"):
            path = "/tmp/%s.%s" % (c, ext)
            try:
                raw = open(path, "rb").read()
            except OSError:
                sys.exit("MISSING: %s" % path)
            if ext == "token":
                raw = raw.rstrip(b"\n")
            data["%s.%s" % (c, ext)] = base64.b64encode(raw).decode()
    manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": NAME, "namespace": NS},
        "type": "Opaque",
        "data": data,
    }
    out = "/tmp/wkc-merged.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(manifest, f)
    os.chmod(out, 0o600)
    try:
        kubectl("apply", "-f", out)
    finally:
        os.unlink(out)
    print("MERGE_OK clusters=%s" % ",".join(clusters))
    print("KEYS: %s" % ",".join(sorted(data)))


if __name__ == "__main__":
    main()
