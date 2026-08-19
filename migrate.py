"""One-shot migration + seed runner for fd-proxy-service (tasks 2.x + 9.2).

Run inside the fd-proxy-service image (the shell pod, or the migrate Job):

    kubectl -n scraw exec -i proxy-migrate-shell -- python - < migrate.py

Three idempotent steps, each printing a clear marker so a failure pinpoints the
exact stage:

  1. ``Base.metadata.create_all`` — creates ban_rules / rate_limits / probes
     (and any other new table) if absent. No-op on the existing ``proxies``
     table (create_all never emits ALTER).
  2. ``migrations/001_add_provider_cols.sql`` — additive ALTER (provider +
     provider_meta) on proxies, backfill gost-*/direct rows onto their owner,
     retire the dead free-proxy rows.
  3. ``seed_all`` — gost-own provider address (127.0.0.1:30080, from
     ``FD_EGRESS_AUTH``) + ban_rules + rate_limits + probes rows.

Step 2 runs the *whole* .sql file through psycopg2's **raw connection** rather
than SQLAlchemy's ``text()``/``exec_driver_sql``. psycopg2's
``cursor.execute(script)`` natively parses ``--`` line comments and
``;``-separated multi-statement scripts (the same server-side parser ``psql -f``
uses), so we sidestep two fragilities that bit earlier attempts:
  - ``sql.split(';')`` broke on a ``;`` inside a ``--`` comment, producing a
    comment-only fragment psycopg2 rejects as an empty query;
  - ``exec_driver_sql`` forwards an ``immutabledict`` as params, which this
    psycopg2/SQLAlchemy combo rejects as "immutabledict is not a sequence".

Step 3 TRUNCATE+RESTART IDENTITY's the three config tables first. They
pre-exist from the OLD fd-open-data-mcp seed with natural keys that don't
match this service's seed keys (e.g. ``rule_type`` naming), so the idempotent
``filter_by(...).first()`` upsert falls through to INSERT and collides on a
desynced id sequence (``UniqueViolation ... id=(1) already exists``). A clean
slate yields a correctly-keyed config set whose rows match the seed's filter
keys — future re-runs update-in-place. ``proxies`` is NOT truncated (circuit
keys depend on stable proxy_ids); only its id sequence is advanced to max(id)
so the gost-own INSERT lands past existing rows.

A pre-step prints the live table + proxies-column state ("before"); a
post-step prints the "after" picture (provider/label/status counts) so the
operator can verify idempotency + stable proxy_ids at a glance.
"""
from __future__ import annotations

import pathlib
import sys

import sqlalchemy
from sqlalchemy import text


def _diag(eng: sqlalchemy.engine.Engine, tag: str) -> None:
    print(f"--- {tag} ---")
    with eng.connect() as c:
        tabs = [
            r[0]
            for r in c.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='public' ORDER BY 1"
                )
            )
        ]
        print("TABLES:", tabs)
        try:
            cols = [
                r[0]
                for r in c.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name='proxies' ORDER BY ordinal_position"
                    )
                )
            ]
            print("PROXIES_COLS:", cols)
            rows = list(
                c.execute(
                    text(
                        "SELECT provider, label, status, count(*) AS n "
                        "FROM proxies GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"
                    )
                )
            )
            print("PROXIES_ROWS:")
            for r in rows:
                print("  ", tuple(r))
        except Exception as e:  # noqa: BLE001 - diagnostic
            print("PROXIES_DIAG_ERR:", repr(e))


def _advance_sequences(eng: sqlalchemy.engine.Engine) -> list[str]:
    """Advance each table's ``id`` sequence to ``max(id)``.

    The config tables get TRUNCATE+RESTART IDENTITY (next call) so their reset
    is cosmetic, but ``proxies`` is NOT truncated — its sequence is stuck below
    ``max(id)`` (the old free-proxy pool inserted dozens of rows with explicit
    ids that bypassed nextval), and the gost-own INSERT would collide. ``setval``
    makes the next ``nextval`` = max+1. ``COALESCE`` keeps it safe on an empty
    table; ``pg_get_serial_sequence`` returning NULL (non-serial col) is skipped.
    """
    tables = ["ban_rules", "source_rate_limits", "source_probes", "proxies"]
    synced = []
    with eng.begin() as c:
        for t in tables:
            seqname = c.execute(
                text("SELECT pg_get_serial_sequence(:t, 'id')"), {"t": t}
            ).scalar()
            if not seqname:
                continue
            c.execute(
                text(
                    f"SELECT setval('{seqname}', "
                    f"COALESCE((SELECT max(id) FROM {t}), 1), true)"
                )
            )
            synced.append(t)
    return synced


def main() -> int:
    print("START migrate")
    from fd_proxy_service.db import get_engine, get_session
    from fd_proxy_service.models import Base
    from fd_proxy_service import seed as seedmod

    print("IMPORT_OK")
    eng = get_engine()
    # Don't print the full URL (it carries the DB password); host+port only.
    try:
        print("ENGINE_HOST:", eng.url.host, "PORT:", eng.url.port, "DB:", eng.url.database)
    except Exception:  # noqa: BLE001
        pass

    _diag(eng, "BEFORE")

    # 1. create NEW tables only (ban_rules/rate_limits/probes if absent). Skips
    #    the existing proxies table — create_all never emits ALTER.
    Base.metadata.create_all(eng)
    print("CREATE_ALL_OK")

    # 2. additive ALTER + backfill + retire. The SQL ships in the image at
    #    /app/migrations/ (COPY . /app in the Dockerfile). Run the WHOLE file
    #    through psycopg2's raw connection: cursor.execute(script) parses `--`
    #    comments + `;` separators server-side (same as `psql -f`), so no
    #    client-side splitting / param-handling is involved. `ADD COLUMN IF NOT
    #    EXISTS` + the `provider IS NULL` guards make the file idempotent, so
    #    re-running after a partial run (the two ALTERs already applied) just
    #    fills in the backfill + retire + verify.
    sqlpath = pathlib.Path("/app/migrations/001_add_provider_cols.sql")
    print("SQLPATH:", sqlpath, "exists:", sqlpath.exists())
    if not sqlpath.exists():
        print(
            "MIGRATION_FILE_MISSING. SQL files on image:",
            [str(p) for p in pathlib.Path("/app").rglob("*.sql")],
        )
        return 2
    sql = sqlpath.read_text()
    raw = eng.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(sql)
        raw.commit()
        cur.close()
    finally:
        raw.close()
    print("MIGRATION_OK")

    # 2b. advance id sequences past existing rows (proxies especially).
    print("SEQ_SYNCED:", _advance_sequences(eng))

    # 3. seed gost-own provider + ban_rules/rate_limits/probes.
    #    The three config tables already hold rows seeded by the OLD
    #    fd-open-data-mcp proxy path, with natural keys that don't match this
    #    service's seed keys AND a desynced id sequence. The desync is subtle:
    #    PostgreSQL ``nextval`` is NON-transactional, so a prior failed INSERT
    #    (last attempt collided at id=1) still consumed sequence value 1,
    #    leaving the sequence at 2 while the table keeps its old rows — so the
    #    idempotent ``filter_by(...).first()`` upsert falls through to INSERT and
    #    the next ``nextval`` (2) collides with the surviving id=2 row.
    #
    #    Fix: TRUNCATE + RESTART IDENTITY in its OWN committed transaction FIRST
    #    (``eng.begin()`` commits on clean exit), then seed in a separate
    #    session. Earlier the TRUNCATE shared the seed session's transaction;
    #    the seed's autoflush INSERT collided, the whole txn rolled back —
    #    undoing the TRUNCATE too, so the table was never actually emptied.
    #    Splitting the transactions breaks the coupling: the empty table + reset
    #    sequence persist before the seed runs, so every ``nextval`` lands
    #    cleanly past the (now-zero) max id.
    #    ``proxies`` is NOT truncated — circuit keys depend on stable proxy_ids;
    #    its sequence was advanced to max(id) in step 2b so the gost-own INSERT
    #    lands past existing rows.
    with eng.begin() as c:
        c.execute(text(
            "TRUNCATE TABLE ban_rules, source_rate_limits, source_probes "
            "RESTART IDENTITY CASCADE"
        ))
    print("TRUNCATE_OK")

    with get_session() as s:
        seedmod.seed_all(s)  # commits internally
    print("SEED_OK")

    _diag(eng, "AFTER")
    print("DONE")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback as tb

        tb.print_exc()
        sys.exit(1)
