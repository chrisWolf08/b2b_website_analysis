
WITH base AS (
  SELECT
  PARSE_DATE('%Y%m%d', e.event_date) AS date,
    e.user_pseudo_id,
    CAST((
      SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id'
    ) AS STRING) AS ga_session_id,

    -- Active-user signals
    e.is_active_user,
    SAFE_CAST((
      SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'engagement_time_msec'
    ) AS INT64) AS engagement_time_msec,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'session_engaged') AS session_engaged,
    e.event_name,

    -- Optional: keep if you need to match a specific stream
    -- e.stream_id
  FROM `neogen-ga4-export.analytics_331328809.events_*` AS e
  WHERE _TABLE_SUFFIX BETWEEN '20250820'
                          AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

),

sessionized AS (
  SELECT
    date,
    user_pseudo_id,
    CONCAT(user_pseudo_id, ':', COALESCE(ga_session_id, 'NOSESS')) AS session_key,
    is_active_user,
    engagement_time_msec,
    session_engaged,
    event_name
  FROM base
),

sessions AS (
  SELECT
    date,
    COUNT(DISTINCT session_key) AS sessions
  FROM sessionized
  GROUP BY date
),

active_users AS (
  -- 1) Prefer the native flag when available (export >= 2023-07-17)
  SELECT
    date,
    COUNT(DISTINCT user_pseudo_id) AS active_user
  FROM sessionized
  WHERE is_active_user IS TRUE
  GROUP BY date

  UNION ALL

  -- 2) Fallback for rows where is_active_user is NULL
  SELECT
    date,
    COUNT(DISTINCT user_pseudo_id) AS active_user
  FROM (
    SELECT DISTINCT
      date, user_pseudo_id
    FROM sessionized
    WHERE is_active_user IS NULL
      AND (
        engagement_time_msec > 0
        OR session_engaged = '1'
        OR event_name IN ('first_visit','first_open')
      )
  )
  GROUP BY date
)

SELECT
  COALESCE(a.date, s.date) AS date,
  IFNULL(a.active_user, 0) AS active_user,
  IFNULL(s.sessions, 0)    AS sessions
FROM sessions s
FULL OUTER JOIN active_users a
  ON a.date = s.date
ORDER BY date;
