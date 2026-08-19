"""Minimal gost-own upsert + sequence fix (raw SQL, no ORM).

Bypasses the ORM seed entirely — the config tables (ban_rules /
source_rate_limits / source_probes) already hold working rows from the old
fd-open-data-mcp seed (the crawler + probe job read them; proxy-control does
NOT), so re-seeding them is polish, not a blocker. The ONE thing the new
service needs is its own gost-own address row in `proxies`, and a proxies id
sequence advanced past the existing free-proxy rows so nothing collides later.

Run inside the shell pod:

    cat fix_gost.py | kubectl -n scraw exec -i proxy-migrate-shell -- python -
"""
from __future__ import annotations

import os

from sqlalchemy import text

from fd_proxy_service.db import get_engine


def main() -> int:
    eng = get_engine()
    try:
        print("ENGINE:", eng.url.host, eng.url.port, eng.url.database)
    except Exception:  # noqa: BLE001
        pass

    auth = os.environ.get("FD_EGRESS_AUTH", "fdproxy:CHANGE_ME")

    with eng.begin() as c:
        # before
        rows = list(
            c.execute(
                text(
                    "SELECT id, provider, label, scheme, ip, port, status "
                    "FROM proxies ORDER BY id"
                )
            )
        )
        print("PROXIES_BEFORE:", len(rows))
        for r in rows:
            print("  ", tuple(r))

        # upsert the gost-own address. label is the idempotency key (enforced at
        # app level, not a DB unique constraint), so select-then-ins/upd.
        existing = c.execute(
            text("SELECT id FROM proxies WHERE label = 'gost-own-local'")
        ).first()
        if existing:
            c.execute(
                text(
                    "UPDATE proxies "
                    "SET scheme='http', ip='127.0.0.1', port=30080, auth=:a, "
                    "    status='active', provider='gost-own', provider_meta=NULL "
                    "WHERE label='gost-own-local'"
                ),
                {"a": auth},
            )
            print("GOST_OWN updated id", existing[0])
        else:
            c.execute(
                text(
                    "INSERT INTO proxies "
                    "  (scheme, ip, port, auth, status, label, provider, "
                    "   provider_meta, created_at) "
                    "VALUES ('http','127.0.0.1',30080,:a,'active',"
                    "       'gost-own-local','gost-own',NULL,now())"
                ),
                {"a": auth},
            )
            newid = c.execute(
                text("SELECT id FROM proxies WHERE label='gost-own-local'")
            ).scalar()
            print("GOST_OWN inserted id", newid)

        # advance the proxies id sequence past max(id) — the old free-proxy pool
        # inserted rows with explicit ids that bypassed nextval, leaving the
        # sequence stuck below max, so the next ORM insert would collide.
        c.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('proxies','id'), "
                "COALESCE((SELECT max(id) FROM proxies), 1), true)"
            )
        )
        print("PROXIES_SEQ advanced to max(id)")

        # also advance the config-table sequences (cosmetic — they're not
        # re-seeded here, but keeps a future CLI seed from colliding).
        for t in ("ban_rules", "source_rate_limits", "source_probes"):
            seq = c.execute(
                text("SELECT pg_get_serial_sequence(:t,'id')"), {"t": t}
            ).scalar()
            if seq:
                c.execute(
                    text(
                        f"SELECT setval('{seq}', "
                        f"COALESCE((SELECT max(id) FROM {t}), 1), true)"
                    )
                )
        print("CONFIG_SEQS advanced")

        # confirm the three config tables already have rows (old seed).
        for t in ("ban_rules", "source_rate_limits", "source_probes"):
            n = c.execute(text(f"SELECT count(*) FROM {t}")).scalar()
            print(f"  {t} rows:", n)

        # after
        rows = list(
            c.execute(
                text(
                    "SELECT id, provider, label, scheme, ip, port, status "
                    "FROM proxies ORDER BY id"
                )
            )
        )
        print("PROXIES_AFTER:", len(rows))
        for r in rows:
            print("  ", tuple(r))

    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
