-- Explicit sort_order keeps metric windows chronological (1m, 5m, 15m) instead of alphabetical.

-- name: MetricsWindowCounts :many
-- One index range scan per outcome over the last 15 minutes; the 1m and 5m windows are FILTERs of the
-- same scan. The former form ran 27 correlated subqueries (3 windows x 3 channels x 3 counts): 0.8-1.6 s on
-- 2M rows with ~300k sent in the last 15 minutes, against 127-144 ms now (README section 10).
WITH windows AS (
  SELECT 1 AS sort_order, '1m'::text AS window_name
  UNION ALL
  SELECT 2 AS sort_order, '5m'::text AS window_name
  UNION ALL
  SELECT 3 AS sort_order, '15m'::text AS window_name
),
channels AS (
  SELECT 'sms'::text AS channel
  UNION ALL
  SELECT 'email'::text AS channel
  UNION ALL
  SELECT 'push'::text AS channel
),
sent AS (
  SELECT n.channel,
         count(*) FILTER (WHERE n.sent_at >= now() - interval '1 minute') AS w1,
         count(*) FILTER (WHERE n.sent_at >= now() - interval '5 minutes') AS w5,
         count(*) AS w15
  FROM notifications n
  WHERE n.status = 'sent' AND n.sent_at >= now() - interval '15 minutes'
  GROUP BY n.channel
),
failed AS (
  SELECT n.channel,
         count(*) FILTER (WHERE n.failed_at >= now() - interval '1 minute') AS w1,
         count(*) FILTER (WHERE n.failed_at >= now() - interval '5 minutes') AS w5,
         count(*) AS w15
  FROM notifications n
  WHERE n.status = 'failed' AND n.failed_at >= now() - interval '15 minutes'
  GROUP BY n.channel
),
retrying AS (
  SELECT n.channel, count(*) AS total
  FROM notifications n
  WHERE n.status = 'retrying'
  GROUP BY n.channel
)
SELECT
  w.window_name,
  c.channel,
  COALESCE(CASE w.sort_order WHEN 1 THEN s.w1 WHEN 2 THEN s.w5 ELSE s.w15 END, 0)::bigint AS sent_count,
  COALESCE(CASE w.sort_order WHEN 1 THEN f.w1 WHEN 2 THEN f.w5 ELSE f.w15 END, 0)::bigint AS failed_count,
  COALESCE(r.total, 0)::bigint AS retrying_count
FROM windows w
CROSS JOIN channels c
LEFT JOIN sent s ON s.channel = c.channel
LEFT JOIN failed f ON f.channel = c.channel
LEFT JOIN retrying r ON r.channel = c.channel
ORDER BY w.sort_order, c.channel;

-- name: LatencyPercentiles :one
-- percentile_cont with an array computes p50/p95/p99 from one sort: two scans instead of six
-- (1.5 s -> 0.55 s on 2M rows with ~284k sent in the last 15 minutes).
WITH e2e AS (
  SELECT percentile_cont(ARRAY[0.50, 0.95, 0.99]::double precision[])
           WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000.0) AS p
  FROM notifications
  WHERE status = 'sent' AND sent_at >= now() - interval '15 minutes'
),
provider AS (
  SELECT percentile_cont(ARRAY[0.50, 0.95, 0.99]::double precision[])
           WITHIN GROUP (ORDER BY duration_ms::double precision) AS p
  FROM delivery_attempts
  WHERE started_at >= now() - interval '15 minutes' AND counted = true
)
SELECT
  COALESCE(e2e.p[1], 0)::double precision AS e2e_p50_ms,
  COALESCE(e2e.p[2], 0)::double precision AS e2e_p95_ms,
  COALESCE(e2e.p[3], 0)::double precision AS e2e_p99_ms,
  COALESCE(provider.p[1], 0)::double precision AS provider_p50_ms,
  COALESCE(provider.p[2], 0)::double precision AS provider_p95_ms,
  COALESCE(provider.p[3], 0)::double precision AS provider_p99_ms
FROM e2e CROSS JOIN provider;
