# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math

from faker import Faker
import numpy as np
from pyspark.sql import DataFrame, functions as F, types as T


dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.text("seed", "20261005")
# Scenario file (sites, storyline roles, alert history) and the alert-tier
# thresholds, both supplied by the bundle so they are defined in one place.
dbutils.widgets.text("scenario_file", "")
dbutils.widgets.text("amber_biovolume", "")
dbutils.widgets.text("red_biovolume", "")

# COMMAND ----------

# Parameterised catalog, schema and volume creation. The pipeline publishes into
# the validated and gold schemas, and the model registers into gold, so all three
# must exist before the downstream tasks run.
_cat = dbutils.widgets.get("catalog")
_pfx = dbutils.widgets.get("schema_prefix")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {_cat}")
except Exception as exc:
    if not spark.sql(f"SHOW CATALOGS LIKE '{_cat}'").count():
        raise RuntimeError(
            f"Catalog {_cat} does not exist and could not be created. Create it "
            f"(or pass --var catalog=<existing catalog>) and re-run."
        ) from exc
for _layer in ("raw", "validated", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_cat}.{_pfx}_{_layer}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {_cat}.{_pfx}_raw.landing")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
_required = ["catalog", "schema_prefix", "as_of_date", "scenario_file", "amber_biovolume", "red_biovolume"]
_missing = [name for name in _required if not dbutils.widgets.get(name)]
if _missing:
    raise ValueError(f"Missing required parameters: {', '.join(_missing)}")
as_of = date.fromisoformat(dbutils.widgets.get("as_of_date"))
seed = int(dbutils.widgets.get("seed"))
amber_biovolume = float(dbutils.widgets.get("amber_biovolume"))
red_biovolume = float(dbutils.widgets.get("red_biovolume"))
with open(dbutils.widgets.get("scenario_file"), "rb") as handle:
    scenario_bytes = handle.read()
scenario = json.loads(scenario_bytes)
# Everything besides the seed that shapes the output. The determinism check only
# compares runs with the same fingerprint, so switching scenario is not an error.
input_fingerprint = hashlib.sha256(
    scenario_bytes + f"|{amber_biovolume}|{red_biovolume}".encode()
).hexdigest()[:16]
story = scenario["storyline"]
breach_site = story["breach_site"]
red_site = story["red_site"]
sensor_fault_site = story["sensor_fault_site"]
# Bumped from 4: contact emails now come from the scenario's reserved domain.
generator_version = 5
landing = f"/Volumes/{catalog}/{schema_prefix}_raw/landing"
start_date = as_of - timedelta(days=1094)
days = 1095
quarters_per_day = 96

contacts_cfg = scenario["contacts"]
fake = Faker(contacts_cfg["faker_locale"])
Faker.seed(seed)
rng = np.random.default_rng(seed)

site_fields = ["site_id", "site_name", "site_type", "valley", "river", "council_name", "latitude", "longitude", "base_flow_ml_day"]
site_rows = [
    (index, *(site[field] for field in site_fields))
    for index, site in enumerate(scenario["sites"])
]
n_sites = len(site_rows)
site_schema = "site_index int, site_id string, site_name string, site_type string, valley string, river string, council_name string, latitude double, longitude double, base_flow_ml_day double"
sites = spark.createDataFrame(site_rows, site_schema)


def pseudo_noise(identifier: F.Column, salt: int, scale: float = 1.0) -> F.Column:
    """Stable, partition-independent pseudo-random value in roughly [-1, 1]."""
    return (
        F.pmod(F.xxhash64(identifier, F.lit(seed + salt)), F.lit(2_000_001)).cast("double")
        / F.lit(1_000_000.0)
        - F.lit(1.0)
    ) * F.lit(scale)


def seasonal(day_index: F.Column) -> F.Column:
    # Peaks on the scenario's bloom-season day of year (mid-January for a southern-hemisphere summer).
    current_date = F.date_add(F.lit(start_date.isoformat()).cast("date"), day_index.cast("int"))
    peak = float(scenario["season"]["peak_day_of_year"])
    return F.cos(F.lit(2.0 * math.pi) * (F.dayofyear(current_date) - F.lit(peak)) / F.lit(365.25))


# COMMAND ----------
# Daily weather, including the as-of week's seven-day forecast.
weather_index = spark.range(n_sites * (days + 7)).withColumnRenamed("id", "row_id")
weather = (
    weather_index
    .withColumn("site_index", F.floor(F.col("row_id") / F.lit(days + 7)).cast("int"))
    .withColumn("day_index", F.pmod(F.col("row_id"), F.lit(days + 7)).cast("int"))
    .join(F.broadcast(sites), "site_index")
    .withColumn("date", F.date_add(F.lit(start_date.isoformat()).cast("date"), F.col("day_index")))
    .withColumn("is_forecast", F.col("day_index") >= F.lit(days))
    .withColumn("horizon", F.col("day_index") - F.lit(days - 1))
    .withColumn(
        "air_temp_max_c",
        F.round(
            F.lit(21.0)
            + F.lit(8.5) * seasonal(F.col("day_index"))
            + F.col("site_index") * F.lit(0.14)
            + pseudo_noise(F.col("row_id"), 11, 1.6),
            2,
        ),
    )
    .withColumn(
        "rainfall_mm",
        F.round(F.greatest(F.lit(0.0), F.lit(4.5) + pseudo_noise(F.col("row_id"), 17, 8.0) - F.lit(2.0) * seasonal(F.col("day_index"))), 2),
    )
)

# Script the forecast drivers. The breach site crosses the learned hot/dry/falling-flow regime on day 5.
weather = (
    weather
    .withColumn(
        "air_temp_max_c",
        F.when(
            (F.col("site_id") == breach_site) & F.col("is_forecast"),
            F.element_at(F.array(*[F.lit(value) for value in story["breach_forecast_air_temp_c"]]), F.col("horizon")),
        )
        .when(
            (F.col("site_id") == red_site) & F.col("is_forecast"),
            F.lit(34.0) + F.col("horizon") * F.lit(0.25),
        )
        .when(F.col("is_forecast") & (F.col("valley") == story["warming_valley"]), F.lit(23.0) + F.col("horizon") * F.lit(0.15))
        .otherwise(F.col("air_temp_max_c")),
    )
    .withColumn(
        "rainfall_mm",
        F.when(F.col("is_forecast") & F.col("site_id").isin(breach_site, red_site), F.lit(0.0)).otherwise(F.col("rainfall_mm")),
    )
    .withColumn("forecast_issued_at", F.when(F.col("is_forecast"), F.to_timestamp(F.lit(f"{as_of.isoformat()} 06:00:00"))))
    .select("site_id", "site_name", "valley", "date", "is_forecast", "air_temp_max_c", "rainfall_mm", "forecast_issued_at")
)

# Historical daily flow.
flow_index = spark.range(n_sites * days).withColumnRenamed("id", "row_id")
flow = (
    flow_index
    .withColumn("site_index", F.floor(F.col("row_id") / F.lit(days)).cast("int"))
    .withColumn("day_index", F.pmod(F.col("row_id"), F.lit(days)).cast("int"))
    .join(F.broadcast(sites), "site_index")
    .withColumn("date", F.date_add(F.lit(start_date.isoformat()).cast("date"), F.col("day_index")))
    .withColumn(
        "flow_ml_day",
        F.round(
            F.col("base_flow_ml_day")
            * (F.lit(1.0) - F.lit(0.22) * seasonal(F.col("day_index")) + pseudo_noise(F.col("row_id"), 23, 0.08)),
            2,
        ),
    )
    .withColumn(
        "flow_ml_day",
        F.when(
            (F.col("site_id") == breach_site) & (F.col("day_index") >= days - 7),
            F.col("base_flow_ml_day") * (F.lit(0.92) - (F.col("day_index") - F.lit(days - 7)) * F.lit(0.055)),
        ).otherwise(F.col("flow_ml_day")),
    )
    .select("site_id", "site_name", "valley", "date", "flow_ml_day")
)

# COMMAND ----------
# 15-minute sonde telemetry: exactly n_sites * 1,095 * 96 rows (1,261,440 for 12 sites).
sonde_index = spark.range(n_sites * days * quarters_per_day, numPartitions=96).withColumnRenamed("id", "reading_id")
sonde = (
    sonde_index
    .withColumn("site_index", F.floor(F.col("reading_id") / F.lit(days * quarters_per_day)).cast("int"))
    .withColumn("within_site", F.pmod(F.col("reading_id"), F.lit(days * quarters_per_day)))
    .withColumn("day_index", F.floor(F.col("within_site") / F.lit(quarters_per_day)).cast("int"))
    .withColumn("quarter", F.pmod(F.col("within_site"), F.lit(quarters_per_day)).cast("int"))
    .join(F.broadcast(sites), "site_index")
    .withColumn(
        "reading_ts",
        F.timestamp_seconds(
            F.lit(int(datetime.combine(start_date, time.min, tzinfo=timezone.utc).timestamp()))
            + F.col("day_index") * F.lit(86400)
            + F.col("quarter") * F.lit(900)
        ),
    )
    .withColumn("diurnal", F.sin(F.lit(2.0 * math.pi) * F.col("quarter") / F.lit(96.0) - F.lit(math.pi / 2.0)))
    .withColumn(
        "water_temp_c",
        F.round(F.lit(20.0) + F.lit(7.0) * seasonal(F.col("day_index")) + F.lit(2.2) * F.col("diurnal") + pseudo_noise(F.col("reading_id"), 31, 0.35), 3),
    )
    .withColumn(
        "chlorophyll_a_ug_l",
        F.round(F.greatest(F.lit(0.05), F.lit(3.0) + F.lit(5.0) * F.greatest(F.lit(0.0), seasonal(F.col("day_index"))) + pseudo_noise(F.col("reading_id"), 37, 0.7)), 3),
    )
    .withColumn(
        "phycocyanin_rfu",
        F.round(F.greatest(F.lit(0.05), F.lit(5.0) + F.lit(10.0) * F.greatest(F.lit(0.0), seasonal(F.col("day_index"))) + pseudo_noise(F.col("reading_id"), 41, 1.2)), 3),
    )
    .withColumn("turbidity_ntu", F.round(F.lit(4.2) + pseudo_noise(F.col("reading_id"), 43, 0.35), 3))
    .withColumn("dissolved_oxygen_mg_l", F.round(F.lit(8.1) - F.lit(0.05) * (F.col("water_temp_c") - F.lit(20.0)) + pseudo_noise(F.col("reading_id"), 47, 0.16), 3))
    .withColumn("ph", F.round(F.lit(7.55) + pseudo_noise(F.col("reading_id"), 53, 0.10), 3))
)

# The breach site's real trend changes several correlated signals; the sensor-fault site changes only optical algae channels.
sonde = (
    sonde
    .withColumn(
        "water_temp_c",
        F.when(
            (F.col("site_id") == breach_site) & (F.col("day_index") >= days - 3),
            F.lit(25.0) + (F.col("day_index") - F.lit(days - 3)) * F.lit(2.5) + F.lit(1.2) * F.col("diurnal"),
        ).otherwise(F.col("water_temp_c")),
    )
    .withColumn(
        "phycocyanin_rfu",
        F.when(
            (F.col("site_id") == breach_site) & (F.col("day_index") >= days - 3),
            F.lit(30.0) + (F.col("day_index") - F.lit(days - 3)) * F.lit(35.0) + pseudo_noise(F.col("reading_id"), 59, 0.5),
        )
        .when((F.col("site_id") == sensor_fault_site) & (F.col("day_index") == days - 1), F.lit(145.0) + pseudo_noise(F.col("reading_id"), 61, 1.0))
        .otherwise(F.col("phycocyanin_rfu")),
    )
    .withColumn(
        "chlorophyll_a_ug_l",
        F.when(
            (F.col("site_id") == breach_site) & (F.col("day_index") >= days - 3),
            F.lit(15.0) + (F.col("day_index") - F.lit(days - 3)) * F.lit(15.0) + pseudo_noise(F.col("reading_id"), 67, 0.4),
        )
        .when((F.col("site_id") == sensor_fault_site) & (F.col("day_index") == days - 1), F.lit(82.0) + pseudo_noise(F.col("reading_id"), 71, 0.7))
        .otherwise(F.col("chlorophyll_a_ug_l")),
    )
    # Deterministically seed 0.1% invalid pH readings for a visible drop expectation.
    .withColumn("ph", F.when(F.pmod(F.col("reading_id"), F.lit(1000)) == 0, F.lit(15.0)).otherwise(F.col("ph")))
    .select(
        "site_id", "site_name", "valley", "reading_ts", "water_temp_c", "chlorophyll_a_ug_l",
        "phycocyanin_rfu", "turbidity_ntu", "dissolved_oxygen_mg_l", "ph",
    )
)

# COMMAND ----------
# Weekly lab samples plus explicit current validated samples (the sensor-fault site intentionally has no sample within 3 days).
weeks = 157
lab_index = spark.range(n_sites * weeks).withColumnRenamed("id", "lab_id")
weekly_lab = (
    lab_index
    .withColumn("site_index", F.floor(F.col("lab_id") / F.lit(weeks)).cast("int"))
    .withColumn("week_index", F.pmod(F.col("lab_id"), F.lit(weeks)).cast("int"))
    .withColumn("day_index", F.col("week_index") * F.lit(7))
    .join(F.broadcast(sites), "site_index")
    .filter(~((F.col("site_id") == sensor_fault_site) & (F.col("week_index") == weeks - 1)))
    .withColumn("sample_date", F.date_add(F.lit(start_date.isoformat()).cast("date"), F.col("day_index")))
    .withColumn("sample_ts", F.to_timestamp(F.concat_ws(" ", F.col("sample_date"), F.lit("09:00:00"))))
    .withColumn("seasonal_signal", seasonal(F.col("day_index")))
    .withColumn("air_temp_proxy", F.lit(21.0) + F.lit(8.5) * F.col("seasonal_signal") + F.col("site_index") * F.lit(0.14))
    .withColumn("flow_ratio_proxy", F.lit(1.0) - F.lit(0.22) * F.col("seasonal_signal") + pseudo_noise(F.col("lab_id"), 79, 0.04))
    .withColumn(
        "risk_score",
        F.lit(-1.0)
        + F.lit(0.20) * (F.col("air_temp_proxy") - F.lit(21.0))
        + F.lit(3.0) * (F.lit(1.0) - F.col("flow_ratio_proxy"))
        + pseudo_noise(F.col("lab_id"), 83, 0.18),
    )
    .withColumn(
        "cyanobacteria_biovolume_mm3_l",
        F.when(F.col("risk_score") >= 1.15, F.lit(4.8) + F.abs(pseudo_noise(F.col("lab_id"), 89, 4.5)))
        .when(F.col("risk_score") >= -0.05, F.lit(0.55) + F.abs(pseudo_noise(F.col("lab_id"), 97, 2.8)))
        .otherwise(F.lit(0.08) + F.abs(pseudo_noise(F.col("lab_id"), 101, 0.25))),
    )
    .withColumn(
        "cyanobacteria_biovolume_mm3_l",
        F.when(
            (F.col("site_id") == sensor_fault_site) & (F.col("week_index") == weeks - 2),
            F.lit(float(story["sensor_fault_prior_biovolume"])),
        ).otherwise(F.col("cyanobacteria_biovolume_mm3_l")),
    )
    .withColumn("result_status", F.when(F.pmod(F.col("lab_id"), F.lit(20)) == 0, F.lit("provisional")).otherwise(F.lit("validated")))
)

current_biovolume = story["current_biovolume"]
red_site_sample_time = time.fromisoformat(story["red_site_sample_time"])
current_rows = []
for row in site_rows:
    site_index, site_id, site_name, site_type, valley, river, council_name, lat, lon, base_flow = row
    if site_id == sensor_fault_site:
        continue
    sample_time = red_site_sample_time if site_id == red_site else time(8, 0)
    sample_ts = datetime.combine(as_of, sample_time)
    current_rows.append((site_index, site_id, site_name, site_type, valley, river, council_name, sample_ts, float(current_biovolume[site_id])))

current_schema = "site_index int, site_id string, site_name string, site_type string, valley string, river string, council_name string, sample_ts timestamp, cyanobacteria_biovolume_mm3_l double"
current_lab = spark.createDataFrame(current_rows, current_schema).withColumn("sample_date", F.to_date("sample_ts")).withColumn("result_status", F.lit("validated"))

lab_columns = ["site_id", "site_name", "site_type", "valley", "river", "council_name", "sample_ts", "sample_date", "cyanobacteria_biovolume_mm3_l", "result_status"]
weekly_lab = weekly_lab.select(*lab_columns)
current_lab = current_lab.select(*lab_columns)
lab_samples = (
    weekly_lab.unionByName(current_lab)
    .withColumn(
        "dominant_genus",
        F.when(F.col("cyanobacteria_biovolume_mm3_l") >= red_biovolume, F.lit("Microcystis"))
        .when(F.col("cyanobacteria_biovolume_mm3_l") >= amber_biovolume, F.lit("Dolichospermum"))
        .otherwise(F.lit("Aphanocapsa")),
    )
    .withColumn("microcystin_ug_l", F.round(F.greatest(F.lit(0.0), F.col("cyanobacteria_biovolume_mm3_l") * F.lit(0.32)), 3))
)

# One event per site in each of the three bloom seasons before the as-of date, all
# starting in the scenario's bloom months, comfortably above the 70% gate.
bloom_months = scenario["season"]["bloom_months"]
bloom_rows = []
event_id = 1
for year in range(as_of.year - 3, as_of.year):
    for row in site_rows:
        site_id, site_name, valley = row[1], row[2], row[4]
        month = bloom_months[(row[0] + year) % len(bloom_months)]
        # A season that starts late in the year carries its later months into the next year.
        event_year = year if month >= bloom_months[0] else year + 1
        event_start = date(event_year, month, 4 + (row[0] % 20))
        bloom_rows.append((f"EVT-{event_id:03d}", site_id, site_name, valley, event_start, event_start + timedelta(days=6 + row[0] % 8), "Red" if row[0] % 3 == 0 else "Amber"))
        event_id += 1
bloom_events = spark.createDataFrame(bloom_rows, "event_id string, site_id string, site_name string, valley string, start_date date, end_date date, peak_tier string")

# Faker is restricted to the scenario's reserved example domain and mobile formatting.
councils = []
seen = set()
for row in site_rows:
    council = row[6]
    if council in seen:
        continue
    seen.add(council)
    person = fake.name()
    digits = contacts_cfg.get("mobile_digits", 8)
    mobile = contacts_cfg["mobile_prefix"] + "".join(str(value) for value in rng.integers(0, 10, size=digits))
    email_local = "".join(ch for ch in person.lower().replace(" ", ".") if ch.isalpha() or ch == ".")
    councils.append((council, person, contacts_cfg["role"], mobile, f"{email_local}@{contacts_cfg['email_domain']}"))
council_contacts = spark.createDataFrame(councils, "council_name string, contact_name string, contact_role string, contact_phone string, contact_email string")

# Alert history from the scenario. `actioned_at` is when an officer notified the
# council or acknowledged the alert; the app seeds its notification log from it.
site_by_id = {row[1]: row for row in site_rows}


def _at(moment: dict | None) -> datetime | None:
    if moment is None:
        return None
    return datetime.combine(as_of - timedelta(days=moment["days_ago"]), time.fromisoformat(moment["time"]))


alert_rows = []
for alert in scenario["alert_history"]:
    actioned_at = _at(alert["actioned"])
    alert_rows.append((
        alert["alert_id"],
        alert["site_id"],
        alert["alert_type"],
        alert["tier"],
        _at(alert["raised"]),
        site_by_id[alert["site_id"]][6],
        f"{alert['recipient']}@{contacts_cfg['email_domain']}",
        alert["message"],
        alert["status"],
        actioned_at if alert["status"] == "council_notified" else None,
        actioned_at,
    ))
alert_history_seed = spark.createDataFrame(alert_rows, "alert_id string, site_id string, alert_type string, tier string, raised_at timestamp, council_name string, recipients string, message string, status string, notified_at timestamp, actioned_at timestamp")

# COMMAND ----------
datasets: dict[str, DataFrame] = {
    "sites": sites.drop("site_index"),
    "sonde_readings": sonde,
    "lab_samples": lab_samples,
    "weather_daily": weather,
    "river_flow_daily": flow,
    "bloom_events": bloom_events,
    "alert_history_seed": alert_history_seed,
    "council_contacts": council_contacts,
}

previous_manifest = {}
try:
    for row in spark.read.parquet(f"{landing}/_generation_manifest").collect():
        fields = row.asDict()
        previous_manifest[fields["dataset"]] = (
            fields["generator_version"], fields["seed"], fields.get("input_fingerprint"),
            fields["row_count"], fields["content_hash"],
        )
except Exception:
    pass

manifest_rows = []
for name, frame in datasets.items():
    frame.write.mode("overwrite").parquet(f"{landing}/{name}")
    materialised = spark.read.parquet(f"{landing}/{name}")
    hash_columns = [F.col(column) for column in sorted(materialised.columns)]
    summary = materialised.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.pmod(F.xxhash64(*hash_columns), F.lit(1_000_000_007))).cast("string").alias("content_hash"),
    ).first()
    current = (generator_version, seed, input_fingerprint, summary["row_count"], summary["content_hash"])
    comparable_previous = name in previous_manifest and previous_manifest[name][:3] == current[:3]
    if comparable_previous and previous_manifest[name] != current:
        raise AssertionError(f"Non-deterministic output for {name}: {previous_manifest[name]} != {current}")
    verified_against_previous = comparable_previous and previous_manifest[name] == current
    manifest_rows.append((name, generator_version, seed, input_fingerprint, summary["row_count"], summary["content_hash"], verified_against_previous, datetime.now(timezone.utc).replace(tzinfo=None)))
    print(f"{name}: rows={summary['row_count']} hash={summary['content_hash']}")

spark.createDataFrame(manifest_rows, "dataset string, generator_version int, seed long, input_fingerprint string, row_count long, content_hash string, verified_against_previous boolean, generated_at timestamp").write.mode("overwrite").parquet(f"{landing}/_generation_manifest")
print(f"PASS deterministic generator seed={seed} landing={landing}")
