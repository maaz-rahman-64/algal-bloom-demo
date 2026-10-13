-- 7-day "chance of Red" forecast per site for a region, from the model serving
-- table. Swap the FROM table to the Genie/model-generated table when ready —
-- the columns (site_id, horizon_day, forecast_date, red_probability,
-- forecast_tier, as_of_date, scored_at, model_*) are the contract the app reads.
-- @param valley STRING
SELECT
  f.site_id,
  f.horizon_day,
  CAST(f.forecast_date AS STRING) AS forecast_date,
  f.red_probability,
  f.forecast_tier,
  CAST(f.as_of_date AS STRING) AS as_of_date,
  CAST(f.scored_at AS STRING) AS scored_at,
  f.model_name,
  f.model_alias,
  f.model_version
FROM {{catalog}}.{{schema_prefix}}_validated.site_forecast_daily f
JOIN {{catalog}}.{{schema_prefix}}_validated.sites s ON s.site_id = f.site_id
WHERE s.valley = :valley
ORDER BY f.site_id, f.horizon_day
