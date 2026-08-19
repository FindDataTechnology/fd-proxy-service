#!/usr/bin/env python3
"""Substitute credential placeholders in proxy-fw-job.yaml (run on guangzhou-xinru).

Usage (on xinru, as ubuntu):
    sudo cat /etc/fd-open-data/db-credentials.env | python3 scripts/sub_pfw.py

Reads the credentials env from stdin (so secrets never appear in argv or ps),
substitutes __REDIS_PW__ / __PG_PW__ in /tmp/proxy-fw-job.yaml, and writes
/tmp/pfw-sub.yaml with mode 0600. Prints only SUB_OK + byte count, never the
secret values. The output is relayed to each worker cluster and applied there
(the crawl-worker SA cannot mount Secrets, so the forwarder Job carries plain
env values by design — see k8s/proxy-fw-job.yaml).
"""
import os
import sys

env = {}
for line in sys.stdin:
    line = line.strip()
    if line and "=" in line and not line.startswith("#"):
        key, val = line.split("=", 1)
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]  # tolerate quoted values (KEY="...")
        env[key.strip()] = val

src = open("/tmp/proxy-fw-job.yaml", encoding="utf-8").read()
if "redis://:__REDIS_PW__@100.64.0.3:30380/1" not in src:
    sys.exit("SUB_FAIL: /tmp/proxy-fw-job.yaml lacks the REDIS placeholder (wrong file?)")
src = src.replace(
    "redis://:__REDIS_PW__@100.64.0.3:30380/1",
    "redis://:%s@100.64.0.3:30380/1" % env["REDIS_PASS"],
)
src = src.replace(
    "postgresql+psycopg2://fd:__PG_PW__@100.64.0.3:30432/fd_open_data",
    "postgresql+psycopg2://%s:%s@100.64.0.3:30432/%s"
    % (env["PGUSER"], env["PGPASSWORD"], env["PGDATABASE"]),
)
if "redis://:__" in src or ":__PG_PW__" in src:
    sys.exit("SUB_FAIL: placeholder remains after substitution")
with open("/tmp/pfw-sub.yaml", "w", encoding="utf-8") as f:
    f.write(src)
os.chmod("/tmp/pfw-sub.yaml", 0o600)
print("SUB_OK bytes=%d" % len(src))
