-- 002_mihomo_routing_concurrency.sql  (add-mihomo-proxy-provider)
--
-- Additive, idempotent migration. Two provider-agnostic mechanisms:
--   1. source_provider_routes — ordered per-source provider preference.
--   2. proxies.max_concurrency — per-address global in-flight cap
--      (NULL = use the provider's default, itself NULL = uncapped).
--
-- No data backfill and no destructive change: an absent route row means "use
-- the global priority ladder", and a NULL cap means "provider default". The
-- migration is therefore inert until seed writes rows.

CREATE TABLE IF NOT EXISTS source_provider_routes (
  source     TEXT PRIMARY KEY,
  providers  JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE proxies ADD COLUMN IF NOT EXISTS max_concurrency INTEGER;

-- Verify (operator reads this; re-runs stay a no-op)
SELECT count(*) AS route_rows FROM source_provider_routes;
SELECT count(*) AS proxies_with_cap FROM proxies WHERE max_concurrency IS NOT NULL;
