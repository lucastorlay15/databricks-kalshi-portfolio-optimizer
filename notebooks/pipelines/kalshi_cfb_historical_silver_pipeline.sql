-- ============================================================
-- Phase 2: Kalshi College Football Silver Layer
--
-- Pipeline target:
--   Catalog: databricks_realtime_optimization
--   Schema:  cfb_analytics
--
-- Source:
--   databricks_realtime_optimization.cfb_analytics
--     .bronze_kalshi_cfb_trades
--
-- IMPORTANT:
-- Confirm these Bronze column mappings:
--   ticker   = Kalshi market ticker
--   trade_ts = timestamp of the trade
--   price    = executed trade price
--   count    = contracts traded
-- ============================================================


-- ============================================================
-- 1. ONE-MINUTE CANDLE TABLE
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_kalshi_cfb_trade_candles_1min
COMMENT 'Continuous one-minute Kalshi CFB trade candles with OHLC, volume, VWAP, trade counts, and forward-filled inactive minutes.'
AS

WITH normalized_trades AS (
    SELECT
        ticker,

        CAST(trade_ts AS TIMESTAMP) AS trade_ts,

        -- Every trade is assigned to its one-minute bucket.
        DATE_TRUNC('MINUTE', CAST(trade_ts AS TIMESTAMP)) AS minute_ts,

        CAST(price AS DOUBLE) AS trade_price,
        CAST(count AS DOUBLE) AS trade_volume

    FROM databricks_realtime_optimization.cfb_analytics.bronze_kalshi_cfb_trades

    WHERE ticker IS NOT NULL
      AND trade_ts IS NOT NULL
      AND price IS NOT NULL
      AND count IS NOT NULL
      AND count > 0
),


-- Aggregate actual trades into observed one-minute candles.
observed_candles AS (
    SELECT
        ticker,
        minute_ts,

        -- Ordered OHLC values.
        MIN_BY(trade_price, trade_ts) AS open_price,
        MAX(trade_price) AS high_price,
        MIN(trade_price) AS low_price,
        MAX_BY(trade_price, trade_ts) AS close_price,

        SUM(trade_volume) AS volume,

        SUM(trade_price * trade_volume)
            / NULLIF(SUM(trade_volume), 0) AS vwap,

        COUNT(*) AS trade_count

    FROM normalized_trades
    GROUP BY
        ticker,
        minute_ts
),


-- Determine the currently observed time boundaries for each market.
market_boundaries AS (
    SELECT
        ticker,
        MIN(minute_ts) AS first_minute_ts,
        MAX(minute_ts) AS last_minute_ts
    FROM observed_candles
    GROUP BY ticker
),


-- Generate every minute between the first and last observed trade.
--
-- sequence() creates the timestamp array and explode() converts the
-- array into one physical row per market per minute.
market_minute_spine AS (
    SELECT
        boundaries.ticker,
        minute_ts

    FROM market_boundaries AS boundaries

    LATERAL VIEW EXPLODE(
        SEQUENCE(
            boundaries.first_minute_ts,
            boundaries.last_minute_ts,
            INTERVAL 1 MINUTE
        )
    ) generated_minutes AS minute_ts
),


-- Join the complete timeline to the observed candles.
timeline_with_gaps AS (
    SELECT
        spine.ticker,
        spine.minute_ts,

        candles.open_price,
        candles.high_price,
        candles.low_price,
        candles.close_price,
        candles.volume,
        candles.vwap,
        candles.trade_count,

        CASE
            WHEN candles.ticker IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS had_trades

    FROM market_minute_spine AS spine

    LEFT JOIN observed_candles AS candles
        ON spine.ticker = candles.ticker
       AND spine.minute_ts = candles.minute_ts
),


-- Carry the most recently observed close forward.
forward_filled AS (
    SELECT
        *,

        LAST(close_price, TRUE) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS previous_observed_close

    FROM timeline_with_gaps
)


SELECT
    ticker,
    minute_ts,

    -- During an inactive minute, the previous close becomes all four
    -- OHLC values. This produces a valid continuous price series.
    COALESCE(open_price, previous_observed_close) AS open_price,
    COALESCE(high_price, previous_observed_close) AS high_price,
    COALESCE(low_price, previous_observed_close) AS low_price,
    COALESCE(close_price, previous_observed_close) AS close_price,

    -- No trade activity occurred during a generated inactive minute.
    COALESCE(volume, 0.0) AS volume,

    -- VWAP is undefined without trades. For a continuous model-ready
    -- series, carry forward the previous market price.
    COALESCE(vwap, previous_observed_close) AS vwap,

    COALESCE(trade_count, 0) AS trade_count,

    had_trades,

    NOT had_trades AS is_inactive_minute,

    -- Useful downstream convenience columns.
    CAST(COALESCE(close_price, previous_observed_close) / 100.0 AS DOUBLE)
        AS close_probability,

    CURRENT_TIMESTAMP() AS silver_updated_at

FROM forward_filled;


-- ============================================================
-- 2. ONE-MINUTE MARKET-STATE FEATURE TABLE
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_kalshi_cfb_market_state_1min
COMMENT 'Minute-level Kalshi CFB modeling features including returns, rolling price statistics, volatility, momentum, and volume statistics.'
AS

WITH candle_history AS (
    SELECT
        *,

        LAG(close_price, 1) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
        ) AS close_lag_1m,

        LAG(close_price, 5) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
        ) AS close_lag_5m,

        LAG(close_price, 15) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
        ) AS close_lag_15m,

        LAG(close_price, 60) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
        ) AS close_lag_60m

    FROM silver_kalshi_cfb_trade_candles_1min
),


returns AS (
    SELECT
        *,

        -- Absolute one-minute movement in Kalshi price points.
        close_price - close_lag_1m AS price_change_1m,

        -- Percentage return.
        CASE
            WHEN close_lag_1m > 0
            THEN (close_price / close_lag_1m) - 1.0
        END AS return_1m,

        -- Log returns are often more appropriate for volatility
        -- calculations and ARIMA-family preprocessing.
        CASE
            WHEN close_price > 0
             AND close_lag_1m > 0
            THEN LN(close_price / close_lag_1m)
        END AS log_return_1m,

        close_price - close_lag_5m AS momentum_5m,
        close_price - close_lag_15m AS momentum_15m,
        close_price - close_lag_60m AS momentum_60m

    FROM candle_history
),


rolling_features AS (
    SELECT
        *,

        -- ----------------------------------------------------
        -- Rolling price averages
        -- Current minute plus prior N-1 rows.
        -- ----------------------------------------------------

        AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS close_sma_5m,

        AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS close_sma_15m,

        AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS close_sma_60m,


        -- ----------------------------------------------------
        -- Rolling volatility
        -- Standard deviation of one-minute log returns.
        -- ----------------------------------------------------

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volatility_5m,

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volatility_15m,

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volatility_60m,


        -- ----------------------------------------------------
        -- Rolling volume statistics
        -- ----------------------------------------------------

        AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volume_avg_15m,

        STDDEV_SAMP(volume) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volume_stddev_15m,

        SUM(volume) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volume_sum_15m,

        AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volume_avg_60m,

        SUM(volume) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volume_sum_60m,

        SUM(trade_count) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS trade_count_15m,

        AVG(vwap) OVER (
            PARTITION BY ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS vwap_sma_15m

    FROM returns
)


SELECT
    ticker,
    minute_ts,

    open_price,
    high_price,
    low_price,
    close_price,
    close_probability,
    vwap,
    volume,
    trade_count,
    had_trades,
    is_inactive_minute,

    -- Returns and movement
    price_change_1m,
    return_1m,
    log_return_1m,

    -- Momentum
    momentum_5m,
    momentum_15m,
    momentum_60m,

    -- Rolling price averages
    close_sma_5m,
    close_sma_15m,
    close_sma_60m,

    -- Distance from rolling average:
    -- particularly useful for mean-reversion analysis.
    close_price - close_sma_5m AS close_vs_sma_5m,
    close_price - close_sma_15m AS close_vs_sma_15m,
    close_price - close_sma_60m AS close_vs_sma_60m,

    -- Rolling volatility
    volatility_5m,
    volatility_15m,
    volatility_60m,

    -- Volume features
    volume_avg_15m,
    volume_stddev_15m,
    volume_sum_15m,
    volume_avg_60m,
    volume_sum_60m,
    trade_count_15m,

    CASE
        WHEN volume_stddev_15m > 0
        THEN (volume - volume_avg_15m) / volume_stddev_15m
    END AS volume_zscore_15m,

    -- VWAP comparison
    vwap_sma_15m,
    close_price - vwap AS close_vs_vwap,
    close_price - vwap_sma_15m AS close_vs_vwap_sma_15m,

    CURRENT_TIMESTAMP() AS silver_updated_at

FROM rolling_features;