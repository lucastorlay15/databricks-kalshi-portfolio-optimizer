# src/modeling/model_config.py

GOLD_TABLE = (
    "databricks_realtime_optimization."
    "cfb_analytics."
    "gold_kalshi_cfb_market_features"
)

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

ISOLATION_FOREST_PARAMETERS = {
    "n_estimators": 200,
    "contamination": "auto",
    "max_samples": "auto",
    "random_state": 42,
    "n_jobs": -1,
}