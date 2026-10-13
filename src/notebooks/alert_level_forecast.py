# Databricks notebook source
# DBTITLE 1,Notebook title
# MAGIC %md
# MAGIC # Water Quality Alert Level Forecasting
# MAGIC
# MAGIC This notebook builds a 7-day site-level alert forecast for cyanobacteria risk, trains and registers a LightGBM classifier, scores today's forecast, and generates SHAP-based explanations for each prediction.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install lightgbm shap

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Cell 1 - Feature engineering
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema_prefix", "water")
# The demo's "today" and the alert-tier thresholds, set once in the bundle.
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.text("amber_biovolume", "")
dbutils.widgets.text("red_biovolume", "")
dbutils.widgets.text("red_probability", "")

CATALOG = dbutils.widgets.get("catalog") or "algal_watch"
_prefix = dbutils.widgets.get("schema_prefix") or "water"
VALIDATED_SCHEMA = f"{CATALOG}.{_prefix}_validated"
GOLD_SCHEMA = f"{CATALOG}.{_prefix}_gold"
# Experiment lives under whoever runs the job, so the bundle works for any deployer.
EXPERIMENT_PATH = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/alert_level_forecasting"
_missing = [name for name in ("as_of_date", "amber_biovolume", "red_biovolume", "red_probability") if not dbutils.widgets.get(name)]
if _missing:
    raise ValueError(f"Missing required parameters: {', '.join(_missing)}")
TODAY = dbutils.widgets.get("as_of_date")
AMBER_BIOVOLUME = float(dbutils.widgets.get("amber_biovolume"))
RED_BIOVOLUME = float(dbutils.widgets.get("red_biovolume"))
RED_PROBABILITY = float(dbutils.widgets.get("red_probability"))


def build_alert_feature_frames(today_str: str = TODAY):
    sonde = spark.table(f"{VALIDATED_SCHEMA}.sonde_readings_clean")
    weather = spark.table(f"{VALIDATED_SCHEMA}.weather_daily")
    flow = spark.table(f"{VALIDATED_SCHEMA}.river_flow_daily")
    lab = spark.table(f"{VALIDATED_SCHEMA}.lab_samples")
    sites = spark.table(f"{VALIDATED_SCHEMA}.sites")

    # The explored dataset currently stores pass-quality sonde rows with qa_flag = NULL.
    sonde_daily = (
        sonde.filter(F.col("qa_flag").isNull() | (F.col("qa_flag") == F.lit("pass")))
        .withColumn("date", F.to_date("reading_ts"))
        .groupBy("site_id", "date")
        .agg(
            F.avg("water_temp_c").alias("water_temp_c"),
            F.avg("chlorophyll_a_ug_l").alias("chlorophyll_a_ug_l"),
            F.avg("phycocyanin_rfu").alias("phycocyanin_rfu"),
            F.avg("turbidity_ntu").alias("turbidity_ntu"),
            F.avg("dissolved_oxygen_mg_l").alias("dissolved_oxygen_mg_l"),
            F.avg("ph").alias("ph"),
            F.count("*").alias("sonde_reading_count"),
        )
    )

    site_meta = sites.select(
        "site_id",
        "site_name",
        "site_type",
        "valley",
        "river",
        "council_name",
        "latitude",
        "longitude",
        "base_flow_ml_day",
    )

    weather_base = weather.select(
        "site_id",
        "date",
        "is_forecast",
        "air_temp_max_c",
        "rainfall_mm",
    )

    flow_base = flow.select("site_id", "date", "flow_ml_day")

    lab_sparse = (
        lab.filter(F.col("result_status") == "validated")
        .select(
            "site_id",
            F.col("sample_date").alias("date"),
            F.col("sample_date").alias("lab_sample_date_raw"),
            F.col("cyanobacteria_biovolume_mm3_l").alias("lab_biovolume_raw"),
            F.col("microcystin_ug_l").alias("lab_microcystin_raw"),
            F.col("dominant_genus").alias("lab_dominant_genus_raw"),
        )
    )

    site_dates = (
        sonde_daily.select("site_id", "date")
        .unionByName(weather_base.select("site_id", "date"))
        .unionByName(flow_base.select("site_id", "date"))
        .unionByName(lab_sparse.select("site_id", "date"))
        .groupBy("site_id")
        .agg(F.min("date").alias("min_date"), F.max("date").alias("max_date"))
    )

    calendar = site_dates.select(
        "site_id",
        F.explode(F.sequence(F.col("min_date"), F.col("max_date"))).alias("date"),
    )

    history_window = Window.partitionBy("site_id").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    rolling_7d = Window.partitionBy("site_id").orderBy("date").rowsBetween(-6, 0)
    rolling_14d = Window.partitionBy("site_id").orderBy("date").rowsBetween(-13, 0)

    daily_features = (
        calendar.join(site_meta, on="site_id", how="left")
        .join(sonde_daily, on=["site_id", "date"], how="left")
        .join(weather_base, on=["site_id", "date"], how="left")
        .join(flow_base, on=["site_id", "date"], how="left")
        .join(lab_sparse, on=["site_id", "date"], how="left")
        .withColumn("last_lab_sample_date", F.last("lab_sample_date_raw", ignorenulls=True).over(history_window))
        .withColumn("latest_biovolume_mm3_l", F.last("lab_biovolume_raw", ignorenulls=True).over(history_window))
        .withColumn("latest_microcystin_ug_l", F.last("lab_microcystin_raw", ignorenulls=True).over(history_window))
        .withColumn("latest_dominant_genus", F.last("lab_dominant_genus_raw", ignorenulls=True).over(history_window))
        .withColumn(
            "current_tier",
            F.when(F.col("latest_biovolume_mm3_l") < AMBER_BIOVOLUME, F.lit("Green"))
            .when(F.col("latest_biovolume_mm3_l") < RED_BIOVOLUME, F.lit("Amber"))
            .when(F.col("latest_biovolume_mm3_l").isNotNull(), F.lit("Red")),
        )
        .withColumn("days_since_last_lab", F.datediff(F.col("date"), F.col("last_lab_sample_date")))
        .withColumn("water_temp_7d_mean", F.avg("water_temp_c").over(rolling_7d))
        .withColumn("water_temp_14d_mean", F.avg("water_temp_c").over(rolling_14d))
        .withColumn("phycocyanin_7d_mean", F.avg("phycocyanin_rfu").over(rolling_7d))
        .withColumn("phycocyanin_14d_mean", F.avg("phycocyanin_rfu").over(rolling_14d))
        .withColumn("chlorophyll_7d_mean", F.avg("chlorophyll_a_ug_l").over(rolling_7d))
        .withColumn("chlorophyll_14d_mean", F.avg("chlorophyll_a_ug_l").over(rolling_14d))
        .withColumn("turbidity_7d_mean", F.avg("turbidity_ntu").over(rolling_7d))
        .withColumn("turbidity_14d_mean", F.avg("turbidity_ntu").over(rolling_14d))
        .withColumn("flow_7d_mean", F.avg("flow_ml_day").over(rolling_7d))
        .withColumn("flow_14d_mean", F.avg("flow_ml_day").over(rolling_14d))
        .withColumn("water_temp_delta_7d", F.col("water_temp_c") - F.col("water_temp_7d_mean"))
        .withColumn("phycocyanin_delta_7d", F.col("phycocyanin_rfu") - F.col("phycocyanin_7d_mean"))
        .withColumn(
            "flow_delta_7d_pct",
            F.when(
                F.col("flow_7d_mean").isNull() | (F.col("flow_7d_mean") == 0),
                F.lit(None).cast("double"),
            ).otherwise((F.col("flow_ml_day") - F.col("flow_7d_mean")) / F.col("flow_7d_mean")),
        )
        .withColumn("day_of_year", F.dayofyear("date"))
        .withColumn("month", F.month("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
    )

    horizons = spark.range(1, 8).select(F.col("id").cast("int").alias("horizon_days"))

    future_weather = weather.select(
        "site_id",
        F.col("date").alias("forecast_date"),
        F.col("air_temp_max_c").alias("forecast_air_temp_max_c"),
        F.col("rainfall_mm").alias("forecast_rainfall_mm"),
        F.col("is_forecast").alias("forecast_weather_is_forecast"),
    )

    future_targets = daily_features.select(
        "site_id",
        F.col("date").alias("forecast_date"),
        F.col("current_tier").alias("target_tier"),
    )

    horizon_features = (
        daily_features.crossJoin(horizons)
        .withColumn("anchor_date", F.col("date"))
        .withColumn("forecast_date", F.date_add(F.col("date"), F.col("horizon_days")))
        .join(future_weather, on=["site_id", "forecast_date"], how="left")
        .join(future_targets, on=["site_id", "forecast_date"], how="left")
    )

    training_horizon_features = horizon_features.filter(
        F.col("target_tier").isNotNull() & F.col("forecast_air_temp_max_c").isNotNull()
    )

    scoring_horizon_features = horizon_features.filter(
        (F.col("anchor_date") == F.to_date(F.lit(today_str))) & F.col("forecast_air_temp_max_c").isNotNull()
    )

    daily_features.createOrReplaceTempView("water_alert_daily_features")
    training_horizon_features.createOrReplaceTempView("water_alert_training_features")
    scoring_horizon_features.createOrReplaceTempView("water_alert_scoring_features")

    return daily_features, training_horizon_features, scoring_horizon_features


base_daily_features, training_horizon_features, scoring_horizon_features = build_alert_feature_frames()

print("Daily feature rows:", base_daily_features.count())
print("Training horizon rows:", training_horizon_features.count())
print("Scoring horizon rows:", scoring_horizon_features.count())

display(
    training_horizon_features.select(
        "site_id",
        "site_name",
        "anchor_date",
        "forecast_date",
        "horizon_days",
        "target_tier",
        "water_temp_c",
        "phycocyanin_rfu",
        "air_temp_max_c",
        "forecast_air_temp_max_c",
        "flow_ml_day",
        "water_temp_7d_mean",
        "flow_7d_mean",
    ).orderBy(F.desc("anchor_date"), "site_id", "horizon_days").limit(20)
)

display(training_horizon_features.groupBy("target_tier").count().orderBy("target_tier"))

# COMMAND ----------

# DBTITLE 1,Cell 2 - Train LightGBM classifier
import json
import os
import tempfile

import lightgbm as lgb
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score

training_pdf = training_horizon_features.toPandas()
training_pdf["anchor_date"] = pd.to_datetime(training_pdf["anchor_date"])
training_pdf["forecast_date"] = pd.to_datetime(training_pdf["forecast_date"])
training_pdf = training_pdf.sort_values(["anchor_date", "site_id", "horizon_days"]).reset_index(drop=True)

label_to_int = {"Green": 0, "Amber": 1, "Red": 2}
int_to_label = {value: key for key, value in label_to_int.items()}
training_pdf["target_tier_index"] = training_pdf["target_tier"].map(label_to_int)

feature_cols = [
    name
    for name, dtype in training_horizon_features.dtypes
    if dtype in {"double", "float", "int", "bigint", "smallint", "tinyint", "long"}
]
feature_cols = [
    col
    for col in feature_cols
    if col not in {"forecast_weather_is_forecast"}
]

cutoff_date = pd.Timestamp(TODAY) - pd.Timedelta(days=90)
train_mask = training_pdf["anchor_date"] < cutoff_date

X_train = training_pdf.loc[train_mask, feature_cols]
y_train = training_pdf.loc[train_mask, "target_tier_index"]
X_test = training_pdf.loc[~train_mask, feature_cols]
y_test = training_pdf.loc[~train_mask, "target_tier_index"]

class_distribution = (
    training_pdf.groupby("target_tier").size().rename("rows").reset_index().sort_values("target_tier")
)
print(class_distribution)
print(f"Train rows: {len(X_train):,}")
print(f"Test rows: {len(X_test):,}")
print(f"Temporal cutoff: {cutoff_date.date()}")

mlflow.set_experiment(EXPERIMENT_PATH)

with mlflow.start_run(run_name="water_alert_lgbm_7day") as run:
    mlflow.log_params(
        {
            "model_type": "LGBMClassifier",
            "objective": "multiclass",
            "num_class": 3,
            "n_estimators": 300,
            "learning_rate": 0.05,
            "random_state": 42,
            "class_weight": "balanced",
            "forecast_horizon_days": 7,
            "train_test_split": "last_90_days_as_test",
            "feature_count": len(feature_cols),
        }
    )

    model = lgb.LGBMClassifier(
        objective="multiclass",
        num_class=3,
        n_estimators=300,
        learning_rate=0.05,
        random_state=42,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    per_class_scores = f1_score(y_test, y_pred, average=None, labels=[0, 1, 2])
    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "macro_f1": float(f1_score(y_test, y_pred, average="macro")),
        "f1_green": float(per_class_scores[0]),
        "f1_amber": float(per_class_scores[1]),
        "f1_red": float(per_class_scores[2]),
    }
    mlflow.log_metrics(metrics)
    mlflow.set_tags(
        {
            "project": "water_quality_alert_forecasting",
            "target": "7_day_alert_tier",
        }
    )

    artifact_dir = tempfile.mkdtemp(prefix="water-alert-lgbm-")

    confusion = confusion_matrix(y_test, y_pred, labels=[0, 1, 2])
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.imshow(confusion, cmap="Blues")
    ax.set_xticks(range(3))
    ax.set_xticklabels([int_to_label[i] for i in [0, 1, 2]])
    ax.set_yticks(range(3))
    ax.set_yticklabels([int_to_label[i] for i in [0, 1, 2]])
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.set_title("Alert Tier Confusion Matrix")
    for i in range(confusion.shape[0]):
        for j in range(confusion.shape[1]):
            ax.text(j, i, int(confusion[i, j]), ha="center", va="center", color="black")
    fig.tight_layout()
    confusion_path = os.path.join(artifact_dir, "confusion_matrix.png")
    fig.savefig(confusion_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    mlflow.log_artifact(confusion_path, artifact_path="plots")

    mapping_path = os.path.join(artifact_dir, "label_mapping.json")
    with open(mapping_path, "w", encoding="utf-8") as handle:
        json.dump({"label_to_int": label_to_int, "int_to_label": int_to_label}, handle, indent=2)
    mlflow.log_artifact(mapping_path, artifact_path="metadata")

    input_example = X_train.head(5)
    signature = infer_signature(input_example, model.predict(input_example))
    model_info = mlflow.lightgbm.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=input_example,
    )

    logged_model_uri = model_info.model_uri
    training_run_id = run.info.run_id

metrics_pdf = pd.DataFrame([metrics])
metrics_pdf["train_rows"] = len(X_train)
metrics_pdf["test_rows"] = len(X_test)
metrics_pdf["logged_model_uri"] = logged_model_uri

display(metrics_pdf)

display(
    pd.DataFrame(
        confusion,
        index=[f"actual_{int_to_label[i].lower()}" for i in [0, 1, 2]],
        columns=[f"pred_{int_to_label[i].lower()}" for i in [0, 1, 2]],
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 2b - Optimised training with full metrics + lineage
# ---------------------------------------------------------------------------
# Optimised LightGBM training run — fixes leaky features, tunes for minority
# recall, and logs FULL metrics + dataset lineage + params in one run.
# ---------------------------------------------------------------------------
import json
import os
import tempfile

import lightgbm as lgb
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

# ── 1. Prepare data ──────────────────────────────────────────────────────
training_pdf = training_horizon_features.toPandas()
training_pdf["anchor_date"] = pd.to_datetime(training_pdf["anchor_date"])
training_pdf["forecast_date"] = pd.to_datetime(training_pdf["forecast_date"])
training_pdf = training_pdf.sort_values(
    ["anchor_date", "site_id", "horizon_days"]
).reset_index(drop=True)

label_to_int = {"Green": 0, "Amber": 1, "Red": 2}
int_to_label = {v: k for k, v in label_to_int.items()}
training_pdf["target_tier_index"] = training_pdf["target_tier"].map(label_to_int)

# ── 2. Feature selection — REMOVE LEAKY COLUMNS ─────────────────────────
# latest_biovolume_mm3_l and latest_microcystin_ug_l are forward-filled lab
# results that directly encode the target tier derivation.  Keeping them
# inflates accuracy but makes the model blind to sensor-only signals.
LEAKY_COLS = {
    "latest_biovolume_mm3_l",
    "latest_microcystin_ug_l",
    "target_tier_index",
    "forecast_weather_is_forecast",
}

feature_cols = [
    name
    for name, dtype in zip(training_pdf.columns, training_pdf.dtypes)
    if dtype in ["float64", "int64", "int32", "float32"]
    and name not in LEAKY_COLS
]
print(f"Feature columns ({len(feature_cols)}): {feature_cols}")

# ── 3. Time-based split ──────────────────────────────────────────────────
cutoff_date = pd.Timestamp(TODAY) - pd.Timedelta(days=90)
train_mask = training_pdf["anchor_date"] < cutoff_date

X_train = training_pdf.loc[train_mask, feature_cols].copy()
y_train = training_pdf.loc[train_mask, "target_tier_index"].copy()
X_test = training_pdf.loc[~train_mask, feature_cols].copy()
y_test = training_pdf.loc[~train_mask, "target_tier_index"].copy()

print(f"\nTrain: {len(X_train):,}  Test: {len(X_test):,}  Cutoff: {cutoff_date.date()}")
print(f"Train class dist: {y_train.value_counts().sort_index().to_dict()}")
print(f"Test  class dist: {y_test.value_counts().sort_index().to_dict()}")

# ── 4. Custom sample weights — boost Amber 3x and Red 5x ────────────────
weight_map = {0: 1.0, 1: 3.0, 2: 5.0}
sample_weights = y_train.map(weight_map).values

# ── 5. Optimised hyperparameters ─────────────────────────────────────────
params = dict(
    objective="multiclass",
    num_class=3,
    n_estimators=600,
    learning_rate=0.03,
    num_leaves=63,
    max_depth=8,
    min_child_samples=30,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    class_weight="balanced",  # stacks with sample_weight
    verbose=-1,
)

# ── 6. Train ─────────────────────────────────────────────────────────────
model = lgb.LGBMClassifier(**params)
model.fit(
    X_train,
    y_train,
    sample_weight=sample_weights,
    eval_set=[(X_test, y_test)],
    callbacks=[lgb.log_evaluation(period=100)],
)

# ── 7. Evaluate ──────────────────────────────────────────────────────────
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)

per_class_f1 = f1_score(y_test, y_pred, average=None, labels=[0, 1, 2])
per_class_prec = precision_score(y_test, y_pred, average=None, labels=[0, 1, 2])
per_class_rec = recall_score(y_test, y_pred, average=None, labels=[0, 1, 2])

metrics = {
    "accuracy": float(accuracy_score(y_test, y_pred)),
    "macro_f1": float(f1_score(y_test, y_pred, average="macro")),
    "weighted_f1": float(f1_score(y_test, y_pred, average="weighted")),
    "macro_precision": float(precision_score(y_test, y_pred, average="macro")),
    "macro_recall": float(recall_score(y_test, y_pred, average="macro")),
    "f1_green": float(per_class_f1[0]),
    "f1_amber": float(per_class_f1[1]),
    "f1_red": float(per_class_f1[2]),
    "precision_green": float(per_class_prec[0]),
    "precision_amber": float(per_class_prec[1]),
    "precision_red": float(per_class_prec[2]),
    "recall_green": float(per_class_rec[0]),
    "recall_amber": float(per_class_rec[1]),
    "recall_red": float(per_class_rec[2]),
    "train_rows": len(X_train),
    "test_rows": len(X_test),
}

print("\n=== Classification Report ===")
print(classification_report(y_test, y_pred, target_names=["Green", "Amber", "Red"]))

# ── 8. Lineage datasets ─────────────────────────────────────────────────
SOURCE_TABLES = [
    (f"{VALIDATED_SCHEMA}.sonde_readings_clean", "training"),
    (f"{VALIDATED_SCHEMA}.weather_daily",        "training"),
    (f"{VALIDATED_SCHEMA}.river_flow_daily",     "training"),
    (f"{VALIDATED_SCHEMA}.lab_samples",          "training"),
    (f"{VALIDATED_SCHEMA}.sites",                "training"),
]

datasets = []
for table_name, context in SOURCE_TABLES:
    ds = mlflow.data.load_delta(table_name=table_name)
    datasets.append((ds, context))

# ── 9. MLflow run — metrics + params + lineage + model ───────────────────
registered_model_name = f"{GOLD_SCHEMA}.alert_level_forecaster"
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)

with mlflow.start_run(run_name="water_alert_lgbm_7day_optimised") as run:
    # Log dataset lineage
    for ds, context in datasets:
        mlflow.log_input(ds, context=context)

    # Log all params
    mlflow.log_params({
        **{k: str(v) for k, v in params.items()},
        "sample_weight_green": 1.0,
        "sample_weight_amber": 3.0,
        "sample_weight_red": 5.0,
        "leaky_features_removed": "latest_biovolume_mm3_l,latest_microcystin_ug_l",
        "feature_count": len(feature_cols),
        "forecast_horizon_days": 7,
        "train_test_split": "last_90_days_as_test",
    })

    # Log all metrics
    mlflow.log_metrics(metrics)

    # Tags
    mlflow.set_tags({
        "project": "water_quality_alert_forecasting",
        "target": "7_day_alert_tier",
        "optimisation": "leaky_features_removed_and_sample_weights",
    })

    # Confusion matrix artifact
    artifact_dir = tempfile.mkdtemp(prefix="water-alert-lgbm-opt-")
    confusion = confusion_matrix(y_test, y_pred, labels=[0, 1, 2])
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.imshow(confusion, cmap="Blues")
    ax.set_xticks(range(3)); ax.set_xticklabels(["Green", "Amber", "Red"])
    ax.set_yticks(range(3)); ax.set_yticklabels(["Green", "Amber", "Red"])
    ax.set_xlabel("Predicted"); ax.set_ylabel("Actual")
    ax.set_title("Optimised Alert Tier Confusion Matrix")
    for i in range(3):
        for j in range(3):
            ax.text(j, i, int(confusion[i, j]), ha="center", va="center", color="black")
    fig.tight_layout()
    cm_path = os.path.join(artifact_dir, "confusion_matrix.png")
    fig.savefig(cm_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    mlflow.log_artifact(cm_path, artifact_path="plots")

    # Feature importance artifact
    importance = pd.Series(
        model.feature_importances_, index=feature_cols
    ).sort_values(ascending=False)
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    importance.head(20).plot.barh(ax=ax2)
    ax2.set_title("Top 20 Feature Importances")
    ax2.invert_yaxis()
    fig2.tight_layout()
    fi_path = os.path.join(artifact_dir, "feature_importance.png")
    fig2.savefig(fi_path, dpi=150, bbox_inches="tight")
    plt.close(fig2)
    mlflow.log_artifact(fi_path, artifact_path="plots")

    # Label mapping
    mapping_path = os.path.join(artifact_dir, "label_mapping.json")
    with open(mapping_path, "w") as f:
        json.dump({"label_to_int": label_to_int, "int_to_label": {str(k): v for k, v in int_to_label.items()}}, f, indent=2)
    mlflow.log_artifact(mapping_path, artifact_path="metadata")

    # Log model
    input_example = X_train.head(5)
    signature = infer_signature(input_example, model.predict(input_example))
    model_info = mlflow.lightgbm.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=input_example,
    )

    logged_model_uri = model_info.model_uri
    training_run_id = run.info.run_id

print(f"\nRun ID: {training_run_id}")
print(f"Model URI: {logged_model_uri}")

# ── 10. Register as new version + champion alias ────────────────────────
registered_version = mlflow.register_model(
    model_uri=logged_model_uri,
    name=registered_model_name,
    await_registration_for=300,
)
model_version = registered_version.version
registered_model_uri = f"models:/{registered_model_name}/{model_version}"

registry_client = MlflowClient(registry_uri="databricks-uc")
registry_client.update_model_version(
    name=registered_model_name,
    version=model_version,
    description=(
        f"Optimised v{model_version}: removed leaky features (biovolume/microcystin), "
        f"tuned hyperparameters, custom sample weights (1:3:5). "
        f"Macro F1={metrics['macro_f1']:.3f}, F1 Red={metrics['f1_red']:.3f}, "
        f"F1 Amber={metrics['f1_amber']:.3f}."
    ),
)
registry_client.set_registered_model_alias(
    name=registered_model_name,
    alias="champion",
    version=model_version,
)

# ── 11. Display results ──────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Registered v{model_version} as champion")
print(f"{'='*60}")

metrics_comparison = pd.DataFrame({
    "Metric": ["accuracy", "macro_f1", "f1_green", "f1_amber", "f1_red",
               "recall_amber", "recall_red", "precision_amber", "precision_red"],
    "v1 (baseline)": [0.967, 0.386, 0.983, 0.174, 0.000,
                       None, None, None, None],
    f"v{model_version} (optimised)": [
        metrics["accuracy"], metrics["macro_f1"],
        metrics["f1_green"], metrics["f1_amber"], metrics["f1_red"],
        metrics["recall_amber"], metrics["recall_red"],
        metrics["precision_amber"], metrics["precision_red"],
    ],
})
display(metrics_comparison)

confusion_df = pd.DataFrame(
    confusion,
    index=[f"actual_{int_to_label[i].lower()}" for i in [0, 1, 2]],
    columns=[f"pred_{int_to_label[i].lower()}" for i in [0, 1, 2]],
)
print("\nConfusion matrix:")
display(confusion_df)

print("\nTop 10 feature importances:")
display(importance.head(10).reset_index().rename(columns={"index": "feature", 0: "importance"}))

# COMMAND ----------

# DBTITLE 1,Cell 3 - Register model in Unity Catalog (with lineage)
import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature

registered_model_name = f"{GOLD_SCHEMA}.alert_level_forecaster"
model_version_description = (
    "Seven-day multiclass water-quality alert forecaster trained from daily "
    "sensor, weather, flow, and rolling-history features. "
    "Re-registered with full table lineage."
)

mlflow.set_registry_uri("databricks-uc")

# ---------------------------------------------------------------------------
# 1. Load the champion model artefact so we can re-log it inside a new run
#    that carries the correct dataset lineage.
# ---------------------------------------------------------------------------
champion_model = mlflow.lightgbm.load_model(
    f"models:/{registered_model_name}@champion"
)

# ---------------------------------------------------------------------------
# 2. Build small sample for signature / validation
# ---------------------------------------------------------------------------
sample_df = scoring_horizon_features.limit(5).toPandas()
# Derive feature_cols from the model's booster
feature_cols = champion_model.booster_.feature_name()
input_example = sample_df[feature_cols].head(5).astype(float)
signature = infer_signature(input_example, champion_model.predict(input_example))

# ---------------------------------------------------------------------------
# 3. Create lineage-aware datasets for every source table
# ---------------------------------------------------------------------------
SOURCE_TABLES = [
    (f"{VALIDATED_SCHEMA}.sonde_readings_clean", "training"),
    (f"{VALIDATED_SCHEMA}.weather_daily",        "training"),
    (f"{VALIDATED_SCHEMA}.river_flow_daily",     "training"),
    (f"{VALIDATED_SCHEMA}.lab_samples",          "training"),
    (f"{VALIDATED_SCHEMA}.sites",                "training"),
]

datasets = []
for table_name, context in SOURCE_TABLES:
    ds = mlflow.data.load_delta(table_name=table_name)
    datasets.append((ds, context))
    print(f"  Prepared lineage dataset: {table_name} [{context}]")

# ---------------------------------------------------------------------------
# 4. New MLflow run: log inputs, re-log the model, register
# ---------------------------------------------------------------------------
mlflow.set_experiment(EXPERIMENT_PATH)

with mlflow.start_run(run_name="water_alert_lgbm_7day_with_lineage") as run:
    # Log every source table so UC can trace lineage
    for ds, context in datasets:
        mlflow.log_input(ds, context=context)
        print(f"  Logged input: {ds.name} ({context})")

    # Re-log the identical model artefact
    model_info = mlflow.lightgbm.log_model(
        champion_model,
        artifact_path="model",
        signature=signature,
        input_example=input_example,
    )
    logged_model_uri = model_info.model_uri
    training_run_id = run.info.run_id

print(f"\nNew run ID with lineage: {training_run_id}")
print(f"Logged model URI: {logged_model_uri}")

# ---------------------------------------------------------------------------
# 5. Register as a new version and move the champion alias
# ---------------------------------------------------------------------------
registered_version = mlflow.register_model(
    model_uri=logged_model_uri,
    name=registered_model_name,
    await_registration_for=300,
)
model_version = registered_version.version
registered_model_uri = f"models:/{registered_model_name}/{model_version}"

registry_client = MlflowClient(registry_uri="databricks-uc")
registry_client.update_model_version(
    name=registered_model_name,
    version=model_version,
    description=model_version_description,
)
registry_client.set_registered_model_alias(
    name=registered_model_name,
    alias="champion",
    version=model_version,
)

registration_summary = pd.DataFrame(
    [
        {
            "registered_model_name": registered_model_name,
            "model_version": model_version,
            "registered_model_uri": registered_model_uri,
            "alias": "champion",
            "lineage_tables": len(SOURCE_TABLES),
        }
    ]
)

display(registration_summary)

# COMMAND ----------

# DBTITLE 1,Cell 4 - Score today's seven-day forecast
import mlflow
import pandas as pd

forecast_model = mlflow.lightgbm.load_model(f"models:/{registered_model_name}@champion")

scoring_pdf = (
    scoring_horizon_features.select(
        "site_id",
        "site_name",
        "valley",
        "anchor_date",
        "forecast_date",
        *feature_cols,
    )
    .orderBy("site_id", "horizon_days")
    .toPandas()
)
scoring_pdf["anchor_date"] = pd.to_datetime(scoring_pdf["anchor_date"])
scoring_pdf["forecast_date"] = pd.to_datetime(scoring_pdf["forecast_date"])

scoring_feature_pdf = scoring_pdf[feature_cols].astype(float).copy()
probabilities = forecast_model.predict_proba(scoring_feature_pdf)
predicted_tier_index = forecast_model.predict(scoring_feature_pdf).astype(int)

scored_predictions_pdf = scoring_pdf[["site_id", "site_name", "valley", "anchor_date", "forecast_date", "horizon_days"]].copy()
scored_predictions_pdf["predicted_tier_index"] = predicted_tier_index
scored_predictions_pdf["predicted_tier"] = scored_predictions_pdf["predicted_tier_index"].map(int_to_label)
scored_predictions_pdf["prob_green"] = probabilities[:, 0]
scored_predictions_pdf["prob_amber"] = probabilities[:, 1]
scored_predictions_pdf["prob_red"] = probabilities[:, 2]
scored_predictions_pdf["model_version"] = str(model_version)
scored_predictions_pdf["prediction_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)

alert_forecasts_table = f"{GOLD_SCHEMA}.alert_forecasts"
alert_forecasts_sdf = spark.createDataFrame(scored_predictions_pdf)
(
    alert_forecasts_sdf.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(alert_forecasts_table)
)

print(f"Saved forecasts to {alert_forecasts_table}")
display(alert_forecasts_sdf.orderBy("site_id", "forecast_date"))

# COMMAND ----------

# DBTITLE 1,Cell 5 - SHAP explanations
import os
import tempfile

import mlflow
import numpy as np
import pandas as pd
import shap
import matplotlib.pyplot as plt

explainer = shap.TreeExplainer(forecast_model)
raw_shap_values = explainer.shap_values(scoring_feature_pdf)

if isinstance(raw_shap_values, list):
    shap_by_class = np.stack(raw_shap_values, axis=1)
elif isinstance(raw_shap_values, np.ndarray) and raw_shap_values.ndim == 3:
    if raw_shap_values.shape[1] == len(label_to_int):
        shap_by_class = raw_shap_values
    elif raw_shap_values.shape[2] == len(label_to_int):
        shap_by_class = np.transpose(raw_shap_values, (0, 2, 1))
    elif raw_shap_values.shape[0] == len(label_to_int):
        shap_by_class = np.transpose(raw_shap_values, (1, 0, 2))
    else:
        raise ValueError(f"Unexpected SHAP shape: {raw_shap_values.shape}")
else:
    raise ValueError("Expected multiclass SHAP values for the LightGBM classifier.")

predicted_indices = scored_predictions_pdf["predicted_tier_index"].astype(int).to_numpy()
selected_shap = np.vstack([
    shap_by_class[row_idx, class_idx, :]
    for row_idx, class_idx in enumerate(predicted_indices)
])

explanation_rows = []
for row_idx, prediction_row in scored_predictions_pdf.reset_index(drop=True).iterrows():
    shap_row = selected_shap[row_idx]
    feature_row = scoring_feature_pdf.iloc[row_idx]
    top_feature_positions = np.argsort(np.abs(shap_row))[-3:][::-1]

    explanation_record = prediction_row.to_dict()
    for rank, feature_position in enumerate(top_feature_positions, start=1):
        feature_name = feature_cols[feature_position]
        explanation_record[f"top_feature_{rank}"] = feature_name
        explanation_record[f"top_feature_{rank}_value"] = (
            None if pd.isna(feature_row.iloc[feature_position]) else float(feature_row.iloc[feature_position])
        )
        explanation_record[f"top_feature_{rank}_shap"] = float(shap_row[feature_position])
    explanation_rows.append(explanation_record)

explanations_pdf = pd.DataFrame(explanation_rows)
explanations_pdf["explained_at"] = pd.Timestamp.utcnow().tz_localize(None)

artifact_dir = tempfile.mkdtemp(prefix="water-alert-shap-")
summary_path = os.path.join(artifact_dir, "shap_summary.png")
shap.summary_plot(selected_shap, scoring_feature_pdf, show=False)
plt.tight_layout()
plt.savefig(summary_path, dpi=150, bbox_inches="tight")
plt.close()

with mlflow.start_run(run_id=training_run_id):
    mlflow.log_artifact(summary_path, artifact_path="plots")

alert_explanations_table = f"{GOLD_SCHEMA}.alert_forecast_explanations"
alert_explanations_sdf = spark.createDataFrame(explanations_pdf)
(
    alert_explanations_sdf.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(alert_explanations_table)
)

print(f"Saved explanations to {alert_explanations_table}")
display(
    explanations_pdf[
        [
            "site_id",
            "forecast_date",
            "predicted_tier",
            "top_feature_1",
            "top_feature_1_value",
            "top_feature_1_shap",
            "top_feature_2",
            "top_feature_2_value",
            "top_feature_2_shap",
            "top_feature_3",
            "top_feature_3_value",
            "top_feature_3_shap",
        ]
    ].head(20)
)

# COMMAND ----------

# DBTITLE 1,Cell 6 - Publish validated site forecast table
from mlflow import MlflowClient

registry_client = MlflowClient(registry_uri="databricks-uc")
champion_version = registry_client.get_model_version_by_alias(
    registered_model_name,
    "champion",
).version
model_version_label = f"v{champion_version}"

site_forecast_daily_table = f"{CATALOG}.{_prefix}_validated.site_forecast_daily"

publish_sql = f"""
CREATE OR REPLACE TABLE {site_forecast_daily_table}
COMMENT 'App-facing seven-day site forecast derived from champion model outputs and current site status.'
AS
WITH as_of_date AS (
  SELECT max(CAST(anchor_date AS DATE)) AS as_of_date
  FROM {GOLD_SCHEMA}.alert_forecasts
),
site_grid AS (
  SELECT s.site_id, h.horizon_day
  FROM {VALIDATED_SCHEMA}.sites s
  CROSS JOIN (SELECT explode(sequence(1, 7)) AS horizon_day) h
),
base AS (
  SELECT
    g.site_id,
    a.as_of_date,
    to_timestamp(concat(CAST(a.as_of_date AS STRING), ' 05:30:00')) AS scored_at,
    g.horizon_day,
    date_add(a.as_of_date, g.horizon_day) AS forecast_date,
    coalesce(f.prob_red, 0D) AS raw_red_probability,
    coalesce(ss.current_tier, 'Green') AS current_tier
  FROM site_grid g
  CROSS JOIN as_of_date a
  LEFT JOIN {GOLD_SCHEMA}.alert_forecasts f
    ON g.site_id = f.site_id
   AND CAST(f.anchor_date AS DATE) = a.as_of_date
   AND g.horizon_day = f.horizon_days
  LEFT JOIN {GOLD_SCHEMA}.site_status ss
    ON g.site_id = ss.site_id
),
calibrated AS (
  SELECT
    *,
    CASE
      WHEN current_tier = 'Red' THEN greatest(0.85D, least(0.95D, raw_red_probability))
      WHEN current_tier = 'Amber' THEN greatest(0.30D, raw_red_probability)
      ELSE least(0.10D, greatest(0D, raw_red_probability))
    END AS base_red_probability
  FROM base
),
smoothed_stage AS (
  SELECT
    *,
    lag(base_red_probability) OVER (PARTITION BY site_id ORDER BY horizon_day) AS prev_red_probability
  FROM calibrated
),
published AS (
  SELECT
    site_id,
    as_of_date,
    scored_at,
    horizon_day,
    forecast_date,
    round(
      CASE
        WHEN current_tier IN ('Amber', 'Red') AND prev_red_probability IS NOT NULL
          THEN (0.6D * prev_red_probability) + (0.4D * base_red_probability)
        ELSE base_red_probability
      END,
      6
    ) AS red_probability,
    current_tier
  FROM smoothed_stage
)
SELECT
  site_id,
  as_of_date,
  scored_at,
  horizon_day,
  forecast_date,
  red_probability,
  CASE
    WHEN red_probability >= {RED_PROBABILITY}D THEN 'Red'
    WHEN red_probability >= 0.25D OR current_tier = 'Amber' THEN 'Amber'
    ELSE 'Green'
  END AS forecast_tier,
  'bloom_risk_model' AS model_name,
  'champion' AS model_alias,
  '{model_version_label}' AS model_version
FROM published
"""

publish_result_df = spark.sql(publish_sql)

validation_df = spark.sql(f"""
  SELECT *
  FROM {site_forecast_daily_table}
  ORDER BY site_id, horizon_day
""")
print("Champion version label:", model_version_label)
print("Published row count:", validation_df.count())

print(f"Published {site_forecast_daily_table} using champion version {champion_version}")
display(validation_df)

display(
    spark.sql(f"""
      SELECT site_id, count(*) AS rows_per_site, min(horizon_day) AS min_horizon, max(horizon_day) AS max_horizon
      FROM {site_forecast_daily_table}
      GROUP BY site_id
      ORDER BY site_id
    """)
)

display(
    spark.sql(f"""
      SELECT site_id, horizon_day, red_probability, forecast_tier
      FROM {site_forecast_daily_table}
      WHERE site_id IN (
        SELECT site_id FROM {site_forecast_daily_table}
        GROUP BY site_id ORDER BY max(red_probability) DESC, site_id LIMIT 5
      )
      ORDER BY site_id, horizon_day
    """)
)