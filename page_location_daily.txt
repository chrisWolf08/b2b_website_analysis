CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_enterprise.page_location_daily` 
AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
    device.web_info.hostname AS hostname,
    user_pseudo_id,
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
    event_name
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101'
    AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)

SELECT
  date,
  page_location,
  market_id,
  hostname,
  COUNT(DISTINCT CONCAT(user_pseudo_id, ga_session_id)) AS sessions,
  COUNTIF(event_name IN ('page_view', 'screen_view')) AS pageviews
FROM base
GROUP BY date, page_location, market_id, hostname
ORDER BY date DESC, pageviews DESC;
