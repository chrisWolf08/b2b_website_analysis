CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_enterprise.page_location_channel_daily` 
AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,  -- custom field
    device.web_info.hostname AS hostname,
    LOWER(traffic_source.source) AS source,
    LOWER(traffic_source.medium) AS medium,
    COALESCE(m.channel_group, 'Other') AS channel_group,
    user_pseudo_id,
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
    event_name
  FROM `neogen-ga4-export.analytics_331328809.events_*` e
  LEFT JOIN `neogen-ga4-export.analytics_331328809.channel_mapping` m
    ON REGEXP_CONTAINS(LOWER(e.traffic_source.medium), m.medium_pattern)
   AND REGEXP_CONTAINS(LOWER(e.traffic_source.source), m.source_pattern)
  WHERE _TABLE_SUFFIX BETWEEN '20250101'
                          AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND e.event_name IN ('page_view', 'screen_view')  -- scope to view events only
)

SELECT
  date,
  market_id,
  hostname,
  source,
  medium,
  channel_group,
  page_location,
  COUNT(DISTINCT CONCAT(user_pseudo_id, ga_session_id)) AS sessions,   -- sessions that viewed this page
  COUNT(*) AS pageviews                                                -- page_view + screen_view
FROM base
GROUP BY date, market_id, hostname, source, medium, channel_group, page_location
ORDER BY date DESC, pageviews DESC;
