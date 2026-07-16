# src/modeling/model_config.py

GOLD_TABLE = (
    "databricks_realtime_optimization."
    "cfb_analytics."
    "gold_kalshi_cfb_market_features"
)

# ============================================================
# Dataset columns
# ============================================================

IDENTIFIER_COLUMNS = [
    "kalshi_ticker",
    "espn_event_id",
    "minute_ts",
    "season",
    "week",
]

FEATURE_COLUMNS = [
    "current_price",
    "price_change_1m",
    "price_change_3m",
    "price_change_5m",
    "relative_volume_5m",
    "selection_volume_imbalance_5m",
    "price_volatility_10m",
    "minutes_from_game_start",
]

EVALUATION_COLUMNS = [
    "future_price_change_1m",
    "future_price_change_3m",
    "future_price_change_5m",
    "future_price_change_10m",
]

ELIGIBILITY_COLUMN = "is_training_eligible_5m"

TARGET_COLUMN = "future_price_change_5m"

# Every row and both paired Kalshi markets for one game must remain
# in the same train, validation, or test split.
GROUP_COLUMN = "espn_event_id"

# ============================================================
# Reproducible game-group split
# ============================================================

TRAIN_FRACTION = 0.64
VALIDATION_FRACTION = 0.16
TEST_FRACTION = 0.20

RANDOM_STATE = 42

# ============================================================
# Candidate supervised models
# ============================================================

RIDGE_PARAMETERS = {
    "alpha": 1.0,
}

HIST_GRADIENT_BOOSTING_PARAMETERS = {
    "learning_rate": 0.05,
    "max_iter": 200,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 50,
    "l2_regularization": 1.0,
    "early_stopping": False,
    "random_state": RANDOM_STATE,
}

# ============================================================
# Opportunity selectivity analysis
# ============================================================

# These are percentiles of predicted five-minute price movement.
# They are not anomaly-score percentiles.
#
# A percentile of 0.90 means:
# select observations whose model prediction is in the top 10%.
OPPORTUNITY_PERCENTILES = [
    0.50,
    0.60,
    0.70,
    0.80,
    0.85,
    0.90,
    0.95,
    0.975,
    0.99,
]

# Adjacent qualifying observations may represent the same opportunity.
# This setting is only for frequency diagnostics in this notebook.
#
# Example:
# qualifying rows for the same ticker less than five minutes apart
# are treated as part of one signal episode.
SIGNAL_EPISODE_GAP_MINUTES = 5

# ============================================================
# Exploratory Isolation Forest configuration
# ============================================================

# Retained so the existing exploratory notebook continues to run.
# Isolation Forest is not the primary supervised model in notebook 02.

ISOLATION_FOREST_PARAMETERS = {
    "n_estimators": 200,
    "contamination": "auto",
    "max_samples": "auto",
    "random_state": RANDOM_STATE,
    "n_jobs": -1,
}

SELECTED_OPPORTUNITY_PERCENTILES = [
    0.95,
    0.975,
    0.99,
]

MODEL_ARTIFACT_VOLUME_PATH = (
    "/Volumes/"
    "databricks_realtime_optimization/"
    "cfb_analytics/"
    "model_artifacts/"
    "supervised_price_model"
)

DECISION_THRESHOLDS_PATH = (
    f"{MODEL_ARTIFACT_VOLUME_PATH}/"
    "decision_thresholds.json"
)

# ============================================================
# Model Registry
# ============================================================

REGISTERED_MODEL_NAME = (
    "databricks_realtime_optimization."
    "cfb_analytics."
    "kalshi_cfb_price_movement_model"
)

MODEL_SELECTION_ARTIFACT_PATH = (
    "/Volumes/"
    "databricks_realtime_optimization/"
    "cfb_analytics/"
    "model_artifacts/"
    "supervised_price_model/"
    "model_selection.json"
)

MODEL_ALIAS = "champion"

TARGET_HORIZON_MINUTES = 5