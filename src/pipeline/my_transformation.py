# Databricks notebook source
"""Serverless Lakeflow Declarative Pipeline: landing -> raw -> validated -> gold."""

from __future__ import annotations

from datetime import date

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window, functions as F


catalog = spark.conf.get("algal.catalog")
prefix = spark.conf.get("algal.schema_prefix")
landing = spark.conf.get("algal.landing_path")
as_of = date.fromisoformat(spark.conf.get("algal.as_of_date"))
# Alert-tier thresholds (cyanobacteria biovolume, mm³/L), set once in the bundle.
amber_biovolume = float(spark.conf.get("algal.amber_biovolume"))
red_biovolume = float(spark.conf.get("algal.red_biovolume"))

raw_schema = f"{prefix}_raw"
validated_schema = f"{prefix}_validated"
gold_schema = f"{prefix}_gold"


def table(schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"


def raw_source(dataset: str):
    def decorator(function):
        return dp.table(
            name=table(raw_schema, dataset),
            comment=f"Unmodified Auto Loader stream for {dataset}",
            table_properties={"quality": "raw", "pipelines.reset.allowed": "true"},
        )(function)
    return decorator


@raw_source("sites")
def raw_sites() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/sites")


@raw_source("sonde_readings")
def raw_sonde_readings() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/sonde_readings")


@raw_source("lab_samples")
def raw_lab_samples() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/lab_samples")


@raw_source("weather_daily")
def raw_weather_daily() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/weather_daily")


@raw_source("river_flow_daily")
def raw_river_flow_daily() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/river_flow_daily")


@raw_source("bloom_events")
def raw_bloom_events() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/bloom_events")


@raw_source("alert_history_seed")
def raw_alert_history_seed() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/alert_history_seed")


@raw_source("council_contacts")
def raw_council_contacts() -> DataFrame:
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(f"{landing}/council_contacts")


# COMMAND ----------
@dp.table(
    name=table(validated_schema, "sites"),
    comment="Validated site reference data",
    table_properties={"quality": "validated"},
)
@dp.expect_or_drop("site_id_is_present", "site_id IS NOT NULL")
def validated_sites() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "sites"))


@dp.table(
    name=table(validated_schema, "sonde_readings"),
    comment="Range-checked 15-minute sonde readings before catch-B QA",
    table_properties={"quality": "validated"},
)
@dp.expect_all_or_drop(
    {
        "sonde_site_complete": "site_id IS NOT NULL AND reading_ts IS NOT NULL",
        "sonde_temperature_range": "water_temp_c BETWEEN 0 AND 45",
        "sonde_ph_range": "ph BETWEEN 4 AND 11",
        "sonde_nonnegative_optics": "chlorophyll_a_ug_l >= 0 AND phycocyanin_rfu >= 0 AND turbidity_ntu >= 0",
    }
)
def validated_sonde() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "sonde_readings"))


@dp.table(
    name=table(validated_schema, "lab_samples"),
    comment="Validated and provisional laboratory samples",
    table_properties={"quality": "validated"},
)
@dp.expect_all_or_drop(
    {
        "lab_site_complete": "site_id IS NOT NULL AND sample_ts IS NOT NULL",
        "lab_biovolume_nonnegative": "cyanobacteria_biovolume_mm3_l >= 0",
        "lab_status_known": "result_status IN ('validated', 'provisional')",
    }
)
def validated_labs() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "lab_samples"))


@dp.table(name=table(validated_schema, "weather_daily"), table_properties={"quality": "validated"})
@dp.expect_all_or_drop(
    {
        "weather_site_complete": "site_id IS NOT NULL AND date IS NOT NULL",
        "weather_temperature_range": "air_temp_max_c BETWEEN -10 AND 55",
        "weather_rain_nonnegative": "rainfall_mm >= 0",
    }
)
def validated_weather() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "weather_daily"))


@dp.table(name=table(validated_schema, "river_flow_daily"), table_properties={"quality": "validated"})
@dp.expect_or_drop("flow_positive", "flow_ml_day > 0")
def validated_flow() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "river_flow_daily"))


@dp.table(name=table(validated_schema, "bloom_events"), table_properties={"quality": "validated"})
@dp.expect_or_drop("bloom_dates_ordered", "end_date >= start_date")
def validated_bloom_events() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "bloom_events"))


@dp.table(name=table(validated_schema, "alert_history_seed"), table_properties={"quality": "validated"})
def validated_alert_history() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "alert_history_seed"))


@dp.table(name=table(validated_schema, "council_contacts"), table_properties={"quality": "validated"})
@dp.expect_all_or_drop(
    {
        "contact_complete": "contact_name IS NOT NULL AND contact_phone IS NOT NULL AND contact_email IS NOT NULL",
        "contact_reserved_domain": "contact_email LIKE '%@%.example'",
    }
)
def validated_contacts() -> DataFrame:
    return spark.readStream.table(table(raw_schema, "council_contacts"))


# COMMAND ----------
@dp.materialized_view(
    name=table(validated_schema, "sonde_readings_with_qa"),
    comment="Sonde readings with rolling 14-day catch-B sensor-fault decision and plain-English reason",
    table_properties={"quality": "validated"},
)
def sonde_readings_with_qa() -> DataFrame:
    readings = spark.read.table(table(validated_schema, "sonde_readings")).withColumn(
        "reading_date", F.to_date("reading_ts")
    )
    labs = (
        spark.read.table(table(validated_schema, "lab_samples"))
        .filter(F.col("result_status") == "validated")
        .select("site_id", "sample_date", "cyanobacteria_biovolume_mm3_l")
    )
    optical_measures = ["chlorophyll_a_ug_l", "phycocyanin_rfu"]
    daily_optical = readings.groupBy("site_id", "reading_date").agg(
        *[
            F.percentile_approx(measure, 0.5, 10_000).alias(f"daily_median_{measure}")
            for measure in optical_measures
        ]
    )
    history = (
        Window.partitionBy("site_id")
        .orderBy(F.col("reading_date").cast("timestamp").cast("long"))
        .rangeBetween(-14 * 86400, -86400)
    )
    daily_stats = daily_optical
    for measure in optical_measures:
        quantiles = F.percentile_approx(F.col(f"daily_median_{measure}"), [0.25, 0.5, 0.75], 10_000).over(history)
        daily_stats = (
            daily_stats
            .withColumn(f"history_q25_{measure}", quantiles[0])
            .withColumn(f"history_median_{measure}", quantiles[1])
            .withColumn(f"history_q75_{measure}", quantiles[2])
            .withColumn(
                f"robust_z_{measure}",
                (F.col(f"daily_median_{measure}") - F.col(f"history_median_{measure}"))
                / F.greatest(
                    (F.col(f"history_q75_{measure}") - F.col(f"history_q25_{measure}")) / F.lit(1.349),
                    F.lit(0.1),
                ),
            )
        )
    week_start = F.date_sub(F.lit(as_of.isoformat()).cast("date"), 6)
    recent_days = daily_stats.filter(F.col("reading_date").between(week_start, F.lit(as_of.isoformat()).cast("date")))
    confirmed_days = recent_days.alias("s").join(
        labs.alias("l"),
        (F.col("s.site_id") == F.col("l.site_id"))
        & (F.abs(F.datediff(F.col("s.reading_date"), F.col("l.sample_date"))) <= 3)
        & (F.col("l.cyanobacteria_biovolume_mm3_l") >= amber_biovolume),
        "left",
    )
    day_columns = [F.col(f"s.{column}") for column in recent_days.columns]
    fault_days = (
        confirmed_days.groupBy(*day_columns)
        .agg(F.max(F.when(F.col("l.site_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("lab_confirmed"))
        .withColumn(
            "fault_day",
            (
                (F.greatest(*[F.col(f"robust_z_{measure}") for measure in optical_measures]) > 6.0)
                & (F.col("lab_confirmed") == 0)
            ),
        )
        .select(
            "site_id",
            "reading_date",
            *[f"robust_z_{measure}" for measure in optical_measures],
            "fault_day",
        )
    )
    recent_flagged = (
        readings.filter(F.col("reading_date").between(week_start, F.lit(as_of.isoformat()).cast("date")))
        .join(fault_days, ["site_id", "reading_date"], "left")
        .withColumn("qa_flag", F.when(F.col("fault_day"), F.lit("SENSOR_FAULT")))
        .withColumn(
            "qa_reason",
            F.when(
                F.col("fault_day"),
                F.lit("Sensor-day quarantined after a robust rolling-median optical spike with no lab confirmation \u2014 probable fouled probe."),
            ),
        )
    )
    older = (
        readings.filter(~F.col("reading_date").between(week_start, F.lit(as_of.isoformat()).cast("date")))
        .withColumn("robust_z_chlorophyll_a_ug_l", F.lit(None).cast("double"))
        .withColumn("robust_z_phycocyanin_rfu", F.lit(None).cast("double"))
        .withColumn("qa_flag", F.lit(None).cast("string"))
        .withColumn("qa_reason", F.lit(None).cast("string"))
    )
    output_columns = [column for column in readings.columns if column != "reading_date"] + [
        "robust_z_chlorophyll_a_ug_l", "robust_z_phycocyanin_rfu", "qa_flag", "qa_reason",
    ]
    return older.select(*output_columns).unionByName(recent_flagged.select(*output_columns))


@dp.materialized_view(
    name=table(validated_schema, "sonde_readings_clean"),
    comment="Range-checked sonde readings with probable sensor faults excluded",
    table_properties={"quality": "validated"},
)
@dp.expect_or_drop("catch_b_sensor_day_is_clean", "qa_flag IS NULL")
def sonde_readings_clean() -> DataFrame:
    return spark.read.table(table(validated_schema, "sonde_readings_with_qa"))


# COMMAND ----------
@dp.materialized_view(
    name=table(gold_schema, "sensor_qa_events"),
    comment="One row per catch-B sensor fault episode",
    table_properties={"quality": "gold"},
)
def sensor_qa_events() -> DataFrame:
    flagged = spark.read.table(table(validated_schema, "sonde_readings_with_qa")).filter(F.col("qa_flag") == "SENSOR_FAULT")
    return (
        flagged.withColumn("event_date", F.to_date("reading_ts"))
        .groupBy("site_id", "site_name", "valley", "event_date", "qa_flag", "qa_reason")
        .agg(
            F.min("reading_ts").alias("episode_start"),
            F.max("reading_ts").alias("episode_end"),
            F.count(F.lit(1)).alias("excluded_readings"),
        )
        .withColumn("event_week", F.date_trunc("week", "episode_start").cast("date"))
    )


@dp.materialized_view(
    name=table(gold_schema, "council_contacts"),
    comment="One synthetic council duty contact per monitored site",
    table_properties={"quality": "gold"},
)
def gold_council_contacts() -> DataFrame:
    sites = spark.read.table(table(validated_schema, "sites"))
    contacts = spark.read.table(table(validated_schema, "council_contacts"))
    return sites.join(contacts, "council_name").select(
        "site_id", "site_name", "valley", "council_name", "contact_name", "contact_role", "contact_phone", "contact_email"
    )


@dp.materialized_view(
    name=table(gold_schema, "site_status"),
    comment="Current validated alert tier and latest clean site conditions",
    table_properties={"quality": "gold"},
)
def gold_site_status() -> DataFrame:
    sites = spark.read.table(table(validated_schema, "sites"))
    labs = spark.read.table(table(validated_schema, "lab_samples")).filter(F.col("result_status") == "validated")
    latest_lab = (
        labs.withColumn("row_number", F.row_number().over(Window.partitionBy("site_id").orderBy(F.col("sample_ts").desc())))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )
    sonde = spark.read.table(table(validated_schema, "sonde_readings_clean"))
    daily_sonde = (
        sonde.withColumn("date", F.to_date("reading_ts"))
        .groupBy("site_id", "date")
        .agg(
            F.avg("water_temp_c").alias("water_temp_c"),
            F.avg("chlorophyll_a_ug_l").alias("chlorophyll_a_ug_l"),
            F.avg("phycocyanin_rfu").alias("phycocyanin_rfu"),
            F.avg("turbidity_ntu").alias("turbidity_ntu"),
            F.avg("dissolved_oxygen_mg_l").alias("dissolved_oxygen_mg_l"),
            F.avg("ph").alias("ph"),
        )
    )
    current = daily_sonde.filter(F.col("date") == F.lit(as_of.isoformat()).cast("date")).alias("current")
    seven_days = daily_sonde.filter(F.col("date") == F.date_sub(F.lit(as_of.isoformat()).cast("date"), 7)).alias("prior7")
    three_days = daily_sonde.filter(F.col("date") == F.date_sub(F.lit(as_of.isoformat()).cast("date"), 3)).alias("prior3")
    flow = spark.read.table(table(validated_schema, "river_flow_daily"))
    current_flow = flow.filter(F.col("date") == F.lit(as_of.isoformat()).cast("date")).alias("flow_now")
    prior_flow = flow.filter(F.col("date") == F.date_sub(F.lit(as_of.isoformat()).cast("date"), 7)).alias("flow_prior")
    excluded = (
        spark.read.table(table(validated_schema, "sonde_readings_with_qa"))
        .filter((F.col("qa_flag") == "SENSOR_FAULT") & (F.col("reading_ts") >= F.to_timestamp(F.lit(f"{as_of.isoformat()} 00:00:00"))))
        .groupBy("site_id")
        .agg(F.count(F.lit(1)).alias("qa_excluded_24h"))
    )
    joined = (
        sites.alias("s")
        .join(latest_lab.alias("lab"), "site_id")
        .join(current, "site_id", "left")
        .join(seven_days, "site_id", "left")
        .join(three_days, "site_id", "left")
        .join(current_flow, "site_id", "left")
        .join(prior_flow, "site_id", "left")
        .join(excluded, "site_id", "left")
    )
    return joined.select(
        F.col("site_id"),
        F.col("s.site_name").alias("site_name"),
        F.col("s.site_type").alias("site_type"),
        F.col("s.valley").alias("valley"),
        F.col("s.river").alias("river"),
        F.col("s.council_name").alias("council_name"),
        F.when(F.col("lab.cyanobacteria_biovolume_mm3_l") >= red_biovolume, F.lit("Red"))
        .when(F.col("lab.cyanobacteria_biovolume_mm3_l") >= amber_biovolume, F.lit("Amber"))
        .otherwise(F.lit("Green")).alias("current_tier"),
        F.col("lab.sample_ts").alias("tier_since"),
        F.col("lab.sample_date").alias("validated_sample_date"),
        F.round(F.col("lab.cyanobacteria_biovolume_mm3_l"), 3).alias("biovolume_mm3_l"),
        F.col("lab.dominant_genus").alias("dominant_genus"),
        F.round(F.col("current.water_temp_c"), 3).alias("water_temp_c"),
        F.round(F.col("current.water_temp_c") - F.col("prior7.water_temp_c"), 3).alias("water_temp_delta_7d"),
        F.round(F.col("flow_now.flow_ml_day"), 3).alias("flow_ml_day"),
        F.round((F.col("flow_now.flow_ml_day") / F.col("flow_prior.flow_ml_day") - 1.0) * 100.0, 3).alias("flow_delta_7d_pct"),
        F.round(F.col("current.phycocyanin_rfu") - F.col("prior3.phycocyanin_rfu"), 3).alias("phycocyanin_trend"),
        F.coalesce(F.col("qa_excluded_24h"), F.lit(0)).alias("qa_excluded_24h"),
        F.current_timestamp().alias("updated_at"),
    )
