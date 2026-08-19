-- 001_add_provider_cols.sql  (add-proxy-service)
--
-- Additive, idempotent migration: add `provider` + `provider_meta` to the
-- `proxies` table so each row is owned by a provider (gost-own, rotate-api, …),
-- backfill existing rows onto their owner, and retire the dead free-proxy rows.
--
-- Existing proxy_ids are NEVER changed — circuit keys (`circuit:{source}:{id}`)
-- depend on stable ids. `ADD COLUMN IF NOT EXISTS` makes re-runs a no-op.
-- `provider IS NULL` guards make the backfill re-runnable too.

-- 2.1 — new columns ----------------------------------------------------------
ALTER TABLE proxies ADD COLUMN IF NOT EXISTS provider      TEXT;
ALTER TABLE proxies ADD COLUMN IF NOT EXISTS provider_meta JSONB;

-- 2.2 — backfill existing rows onto their owning provider -------------------
-- gost hostNetwork egresses registered as `gost-*` → gost-own; scheme=direct
-- rows are the cluster's own egress → direct-egress. Other schemes are left
-- NULL (a provider-classic row) until a provider claims them.
UPDATE proxies SET provider = 'gost-own'     WHERE label LIKE 'gost-%'  AND provider IS NULL;
UPDATE proxies SET provider = 'direct-egress' WHERE scheme = 'direct'   AND provider IS NULL;

-- 2.3 — retire the dead free-proxy rows (closes the long-pending task) ------
UPDATE proxies
   SET status = 'retired', retired_at = now()
 WHERE label = 'free-proxy' AND status = 'active';

-- 2.4 — verify (operator reads this; ids stay stable) ----------------------
SELECT provider, label, status, count(*) AS n
  FROM proxies
 GROUP BY 1, 2, 3
 ORDER BY 1, 2, 3;

-- free-proxy must show 0 active:
SELECT count(*) AS active_free_proxy FROM proxies
 WHERE label = 'free-proxy' AND status = 'active';
