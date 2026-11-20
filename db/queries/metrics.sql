-- Explicit sort_order keeps metric windows chronological (1m, 5m, 15m) instead of alphabetical.

-- name: MetricsWindowCounts :many
WITH windows AS (
  SELECT 1 AS sort_order, '1m'::text AS window_name, now() - interval '1 minute' AS cutoff
  UNION ALL
  SELECT 2 AS sort_order, '5m'::text AS window_name, now() - interval '5 minutes' AS cutoff
  UNION ALL
  SELECT 3 AS sort_order, '15m'::text AS window_name, now() - interval '15 minutes' AS cutoff
),
channels AS (
  SELECT 'sms'::text AS channel
  UNION ALL
  SELECT 'email'::text AS channel
  UNION ALL
  SELECT 'push'::text AS channel
)
SELECT
  w.window_name,
  c.channel,
  (
    SELECT count(*)::bigint
    FROM notifications n
    WHERE n.status = 'sent' AND n.channel = c.channel AND n.sent_at >= w.cutoff
  ) AS sent_count,
  (
    SELECT count(*)::bigint
    FROM notifications n
    WHERE n.status = 'failed' AND n.channel = c.channel AND n.failed_at >= w.cutoff
  ) AS failed_count,
  (
    SELECT count(*)::bigint
    FROM notifications n
    WHERE n.status = 'retrying' AND n.channel = c.channel
  ) AS retrying_count
FROM windows w
CROSS JOIN channels c
ORDER BY w.sort_order, c.channel;

-- name: LatencyPercentiles :one
SELECT
  COALESCE((
    SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000.0)
    FROM notifications
    WHERE status = 'sent' AND sent_at >= now() - interval '15 minutes'
  ), 0)::double precision AS e2e_p50_ms,
  COALESCE((
    SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000.0)
    FROM notifications
    WHERE status = 'sent' AND sent_at >= now() - interval '15 minutes'
  ), 0)::double precision AS e2e_p95_ms,
  COALESCE((
    SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000.0)
    FROM notifications
    WHERE status = 'sent' AND sent_at >= now() - interval '15 minutes'
  ), 0)::double precision AS e2e_p99_ms,
  COALESCE((
    SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms::double precision)
    FROM delivery_attempts
    WHERE started_at >= now() - interval '15 minutes' AND counted = true
  ), 0)::double precision AS provider_p50_ms,
  COALESCE((
    SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms::double precision)
    FROM delivery_attempts
    WHERE started_at >= now() - interval '15 minutes' AND counted = true
  ), 0)::double precision AS provider_p95_ms,
  COALESCE((
    SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms::double precision)
    FROM delivery_attempts
    WHERE started_at >= now() - interval '15 minutes' AND counted = true
  ), 0)::double precision AS provider_p99_ms;
