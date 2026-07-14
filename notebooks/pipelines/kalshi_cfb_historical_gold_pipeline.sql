-- ============================================================
-- Gold Kalshi CFB Market Behavior Features
-- ============================================================
--
-- Purpose:
--   Create a model-ready feature table for predicting short-term
--   Kalshi market-price movement.
--
-- Grain:
--   One row per Kalshi ticker per one-minute timestamp.
--
-- Source:
--   databricks_realtime_optimization.cfb_analytics
--     .silver_kalshi_cfb_trade_candles_1min
--
-- Initial model target:
--   future_price_change_5m
--
-- Initial trade eligibility:
--   - 15 through 105 minutes after scheduled kickoff
--   - Current price from 0.10 through 0.90
--   - At least one contract traded during the previous 5 minutes
--   - At least 5 minutes of prior price history
--
-- Important assumption:
--   Silver contains one row per clock minute, including inactive
--   minutes. This makes LAG(..., 5) and LEAD(..., 5) correspond
--   to approximately five clock minutes.
-- ============================================================


CREATE OR REFRESH MATERIALIZED VIEW gold_kalshi_cfb_market_features
COMMENT
  'Model-ready one-minute Kalshi college-football market features with recent price movement, volume behavior, volatility, short-horizon future labels, and conservative trade-eligibility flags.'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS

-- ------------------------------------------------------------
-- 1. Read and standardize the Silver data
-- ------------------------------------------------------------

WITH source_data AS (

    SELECT
        kalshi_ticker,
        season,
        week,
        espn_event_id,
        game_start_utc,

        away_team,
        away_abbrev_espn,
        away_abbrev_kalshi,

        home_team,
        home_abbrev_espn,
        home_abbrev_kalshi,

        market_side,
        selection_team,
        selection_abbrev_kalshi,
        opponent_team,
        opponent_abbrev_kalshi,

        minute_ts,

        open_price,
        high_price,
        low_price,
        close_price,
        vwap,

        COALESCE(volume, 0.0) AS volume_1m,
        COALESCE(trade_count, 0) AS trade_count_1m,

        COALESCE(selection_buy_trade_count, 0)
            AS selection_buy_trade_count_1m,

        COALESCE(selection_sell_trade_count, 0)
            AS selection_sell_trade_count_1m,

        COALESCE(selection_buy_volume, 0.0)
            AS selection_buy_volume_1m,

        COALESCE(selection_sell_volume, 0.0)
            AS selection_sell_volume_1m,

        COALESCE(block_trade_count, 0)
            AS block_trade_count_1m,

        had_trades,
        is_inactive_minute,

        implied_yes_probability,
        minutes_from_game_start,
        silver_updated_at_utc,

        COALESCE(
            implied_yes_probability,
            close_price
        ) AS observed_price

    FROM
        databricks_realtime_optimization
            .cfb_analytics
            .silver_kalshi_cfb_trade_candles_1min

    WHERE
        kalshi_ticker IS NOT NULL
        AND minute_ts IS NOT NULL
),


-- ------------------------------------------------------------
-- 2. Carry the latest observed price through inactive minutes
-- ------------------------------------------------------------

price_filled AS (

    SELECT
        *,

        LAST_VALUE(observed_price, TRUE) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS current_price

    FROM source_data
),


-- ------------------------------------------------------------
-- 3. Create historical price lags
-- ------------------------------------------------------------

price_lags AS (

    SELECT
        *,

        LAG(current_price, 1) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS price_1m_ago,

        LAG(current_price, 3) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS price_3m_ago,

        LAG(current_price, 5) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS price_5m_ago,

        LAG(current_price, 10) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS price_10m_ago

    FROM price_filled
),


-- ------------------------------------------------------------
-- 4. Calculate recent price movement
-- ------------------------------------------------------------

price_movement AS (

    SELECT
        *,

        current_price - price_1m_ago
            AS price_change_1m,

        current_price - price_3m_ago
            AS price_change_3m,

        current_price - price_5m_ago
            AS price_change_5m,

        current_price - price_10m_ago
            AS price_change_10m,

        ABS(current_price - price_1m_ago)
            AS absolute_price_change_1m,

        ABS(current_price - price_5m_ago)
            AS absolute_price_change_5m

    FROM price_lags
),


-- ------------------------------------------------------------
-- 5. Calculate rolling price and volume behavior
-- ------------------------------------------------------------

rolling_behavior AS (

    SELECT
        *,

        SUM(volume_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volume_5m,

        SUM(volume_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS volume_10m,

        SUM(trade_count_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS trade_count_5m,

        SUM(selection_buy_volume_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS selection_buy_volume_5m,

        SUM(selection_sell_volume_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS selection_sell_volume_5m,

        AVG(volume_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) * 5.0 AS typical_volume_5m,

        STDDEV_SAMP(current_price) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS price_volatility_10m,

        AVG(absolute_price_change_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS average_absolute_move_10m

    FROM price_movement
),


-- ------------------------------------------------------------
-- 6. Create relative volume and order-flow features
-- ------------------------------------------------------------

market_features AS (

    SELECT
        *,

        CASE
            WHEN typical_volume_5m > 0
            THEN volume_5m / typical_volume_5m
            ELSE NULL
        END AS relative_volume_5m,

        selection_buy_volume_5m
            - selection_sell_volume_5m
            AS selection_net_volume_5m,

        CASE
            WHEN (
                selection_buy_volume_5m
                + selection_sell_volume_5m
            ) > 0
            THEN (
                selection_buy_volume_5m
                - selection_sell_volume_5m
            ) / (
                selection_buy_volume_5m
                + selection_sell_volume_5m
            )
            ELSE NULL
        END AS selection_volume_imbalance_5m

    FROM rolling_behavior
),


-- ------------------------------------------------------------
-- 7. Create future prices
--
-- These are historical training labels.
-- They are not model inputs during live inference.
-- ------------------------------------------------------------

future_prices AS (

    SELECT
        *,

        LEAD(current_price, 1) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS future_price_1m,

        LEAD(current_price, 3) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS future_price_3m,

        LEAD(current_price, 5) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS future_price_5m,

        LEAD(current_price, 10) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS future_price_10m

    FROM market_features
),


-- ------------------------------------------------------------
-- 8. Calculate the future price-change labels
-- ------------------------------------------------------------

training_labels AS (

    SELECT
        *,

        future_price_1m - current_price
            AS future_price_change_1m,

        future_price_3m - current_price
            AS future_price_change_3m,

        future_price_5m - current_price
            AS future_price_change_5m,

        future_price_10m - current_price
            AS future_price_change_10m

    FROM future_prices
),


-- ------------------------------------------------------------
-- 9. Calculate safety and model-eligibility flags
-- ------------------------------------------------------------

eligibility_flags AS (

    SELECT
        *,

        COALESCE(
            minutes_from_game_start BETWEEN 15 AND 105,
            FALSE
        ) AS is_early_game_window,

        COALESCE(
            current_price BETWEEN 0.10 AND 0.90,
            FALSE
        ) AS is_probability_eligible,

        COALESCE(
            volume_5m >= 1.0,
            FALSE
        ) AS is_volume_eligible,

        price_5m_ago IS NOT NULL
            AS has_5m_feature_history,

        future_price_5m IS NOT NULL
            AS has_5m_training_label,

        COALESCE(
            minutes_from_game_start BETWEEN 15 AND 105
            AND current_price BETWEEN 0.10 AND 0.90
            AND volume_5m >= 1.0
            AND price_5m_ago IS NOT NULL,
            FALSE
        ) AS is_trade_eligible,

        COALESCE(
            minutes_from_game_start BETWEEN 15 AND 105
            AND current_price BETWEEN 0.10 AND 0.90
            AND volume_5m >= 1.0
            AND price_5m_ago IS NOT NULL
            AND future_price_5m IS NOT NULL,
            FALSE
        ) AS is_training_eligible_5m

    FROM training_labels
)


-- ------------------------------------------------------------
-- 10. Final Gold schema
-- ------------------------------------------------------------

SELECT
    -- Identifiers
    kalshi_ticker,
    season,
    week,
    espn_event_id,

    -- Game context
    game_start_utc,
    away_team,
    away_abbrev_espn,
    away_abbrev_kalshi,
    home_team,
    home_abbrev_espn,
    home_abbrev_kalshi,

    -- Market identity
    market_side,
    selection_team,
    selection_abbrev_kalshi,
    opponent_team,
    opponent_abbrev_kalshi,

    -- Observation timing
    minute_ts,
    minutes_from_game_start,

    -- Current market state
    current_price,
    open_price,
    high_price,
    low_price,
    close_price,
    vwap,
    had_trades,
    is_inactive_minute,

    -- Historical price values
    price_1m_ago,
    price_3m_ago,
    price_5m_ago,
    price_10m_ago,

    -- Recent price movement
    price_change_1m,
    price_change_3m,
    price_change_5m,
    price_change_10m,
    absolute_price_change_1m,
    absolute_price_change_5m,

    -- Volume and trading activity
    volume_1m,
    volume_5m,
    volume_10m,
    trade_count_1m,
    trade_count_5m,
    block_trade_count_1m,
    typical_volume_5m,
    relative_volume_5m,

    -- Selection-side order flow
    selection_buy_trade_count_1m,
    selection_sell_trade_count_1m,
    selection_buy_volume_1m,
    selection_sell_volume_1m,
    selection_buy_volume_5m,
    selection_sell_volume_5m,
    selection_net_volume_5m,
    selection_volume_imbalance_5m,

    -- Volatility
    price_volatility_10m,
    average_absolute_move_10m,

    -- Future historical training labels
    future_price_1m,
    future_price_3m,
    future_price_5m,
    future_price_10m,
    future_price_change_1m,
    future_price_change_3m,
    future_price_change_5m,
    future_price_change_10m,

    -- Safety and eligibility flags
    is_early_game_window,
    is_probability_eligible,
    is_volume_eligible,
    has_5m_feature_history,
    has_5m_training_label,
    is_trade_eligible,
    is_training_eligible_5m,

    -- Lineage
    silver_updated_at_utc,
    CURRENT_TIMESTAMP() AS gold_updated_at_utc

FROM eligibility_flags;