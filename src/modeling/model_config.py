GOLD_TABLE = (
    "databricks_realtime_optimization."
    "cfb_analytics."
    "gold_kalshi_cfb_market_features"
)

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

TARGET_COLUMN = "future_price_change_5m"

MODEL_PARAMETERS = {
    "ridge": {
        "alpha": 1.0,
    },
    "gradient_boosted": {
        "learning_rate": 0.05,
        "max_depth": 4,
        "n_estimators": 200,
    },
    "neural_network": {
        "hidden_layer_sizes": (64, 32),
        "learning_rate_init": 0.001,
        "max_iter": 200,
    },
}