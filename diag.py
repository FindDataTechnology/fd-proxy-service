"""Diagnostic: dump the live state of the config tables + their id sequences.

Run inside the shell pod:

    kubectl -n scraw exec -i proxy-migrate-shell -- python - < diag.py
"""
from fd_proxy_service.db import get_engine
from sqlalchemy import text

eng = get_engine()
with eng.connect() as c:
    for t in ("ban_rules", "source_rate_limits", "source_probes", "proxies"):
        n = c.execute(text(f"SELECT count(*) FROM {t}")).scalar()
        mx = c.execute(text(f"SELECT COALESCE(max(id),0) FROM {t}")).scalar()
        seq = c.execute(
            text("SELECT pg_get_serial_sequence(:t,'id')"), {"t": t}
        ).scalar()
        if seq:
            lv, ic = c.execute(text(f"SELECT last_value, is_called FROM {seq}")).one()
            print(f"{t}: count={n} max(id)={mx} seq={seq} last_value={lv} is_called={ic}")
        else:
            print(f"{t}: count={n} max(id)={mx} seq=NONE(non-serial)")
        rows = list(c.execute(text(f"SELECT id, source FROM {t} ORDER BY id LIMIT 6")))
        print(f"  sample(id,source): {rows}")
    # ban_rules natural keys — do they match seed.py's BAN_RULES keys?
    print("--- ban_rules full dump ---")
    for r in c.execute(text(
        "SELECT id, source, rule_type, pattern FROM ban_rules ORDER BY id"
    )):
        print("  ", tuple(r))
