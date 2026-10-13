-- Distinct regions (valleys) that have monitored sites, for the region selector.
SELECT valley, COUNT(*) AS site_count
FROM {{catalog}}.{{schema_prefix}}_validated.sites
GROUP BY valley
ORDER BY valley
