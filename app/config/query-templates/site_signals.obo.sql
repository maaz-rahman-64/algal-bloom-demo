-- Per-site current signals for a region (valley): latest validated lab result,
-- recent vs prior sonde readings (for trend), sensor QA, and river flow trend.
-- The app derives the alert tier and the 7-day "chance of Red" forecast from these signals.
-- @param valley STRING
WITH s AS (
  SELECT site_id, site_name, site_type, valley, river, council_name, base_flow_ml_day, latitude, longitude
  FROM {{catalog}}.{{schema_prefix}}_validated.sites
  WHERE valley = :valley
),
lab AS (
  SELECT site_id, biovolume, dominant_genus, microcystin, sample_date FROM (
    SELECT site_id,
           cyanobacteria_biovolume_mm3_l AS biovolume,
           dominant_genus,
           microcystin_ug_l AS microcystin,
           sample_date,
           ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY sample_ts DESC) AS rn
    FROM {{catalog}}.{{schema_prefix}}_validated.lab_samples
  ) WHERE rn = 1
),
sonde_asof AS (
  SELECT site_id, MAX(to_date(reading_ts)) AS as_of
  FROM {{catalog}}.{{schema_prefix}}_validated.sonde_readings_with_qa
  GROUP BY site_id
),
sonde AS (
  SELECT r.site_id,
    a.as_of AS sonde_as_of,
    AVG(CASE WHEN to_date(r.reading_ts) >= date_sub(a.as_of, 1) THEN r.water_temp_c END) AS temp_now,
    AVG(CASE WHEN to_date(r.reading_ts) BETWEEN date_sub(a.as_of, 8) AND date_sub(a.as_of, 6) THEN r.water_temp_c END) AS temp_prev,
    AVG(CASE WHEN to_date(r.reading_ts) >= date_sub(a.as_of, 1) THEN r.phycocyanin_rfu END) AS phyco_now,
    AVG(CASE WHEN to_date(r.reading_ts) BETWEEN date_sub(a.as_of, 8) AND date_sub(a.as_of, 6) THEN r.phycocyanin_rfu END) AS phyco_prev,
    SUM(CASE WHEN to_date(r.reading_ts) >= date_sub(a.as_of, 6) AND r.qa_flag IS NOT NULL AND r.qa_flag <> '' THEN 1 ELSE 0 END) AS qa_excluded_7d
  FROM {{catalog}}.{{schema_prefix}}_validated.sonde_readings_with_qa r
  JOIN sonde_asof a ON a.site_id = r.site_id
  GROUP BY r.site_id, a.as_of
),
flow_asof AS (
  SELECT site_id, MAX(date) AS as_of
  FROM {{catalog}}.{{schema_prefix}}_validated.river_flow_daily
  GROUP BY site_id
),
flow AS (
  SELECT f.site_id,
    MAX(CASE WHEN f.date = fa.as_of THEN f.flow_ml_day END) AS flow_now,
    MAX(CASE WHEN f.date = date_sub(fa.as_of, 7) THEN f.flow_ml_day END) AS flow_prev
  FROM {{catalog}}.{{schema_prefix}}_validated.river_flow_daily f
  JOIN flow_asof fa ON fa.site_id = f.site_id
  GROUP BY f.site_id
)
SELECT
  s.site_id, s.site_name, s.site_type, s.valley, s.river, s.council_name, s.base_flow_ml_day,
  s.latitude, s.longitude,
  lab.biovolume, lab.dominant_genus, lab.microcystin,
  CAST(lab.sample_date AS STRING) AS sample_date,
  sonde.temp_now, sonde.temp_prev, sonde.phyco_now, sonde.phyco_prev, sonde.qa_excluded_7d,
  CAST(sonde.sonde_as_of AS STRING) AS sonde_as_of,
  flow.flow_now, flow.flow_prev
FROM s
LEFT JOIN lab ON lab.site_id = s.site_id
LEFT JOIN sonde ON sonde.site_id = s.site_id
LEFT JOIN flow ON flow.site_id = s.site_id
ORDER BY lab.biovolume DESC NULLS LAST
