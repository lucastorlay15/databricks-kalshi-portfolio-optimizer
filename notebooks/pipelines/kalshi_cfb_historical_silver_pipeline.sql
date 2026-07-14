-- ============================================================
-- KALSHI COLLEGE FOOTBALL — SILVER DECLARATIVE PIPELINE
-- ============================================================
--
-- Pipeline target:
--   Catalog: databricks_realtime_optimization
--   Schema:  cfb_analytics
--
-- Bronze source:
--   databricks_realtime_optimization.cfb_analytics
--       .bronze_kalshi_cfb_trades
--
-- Silver outputs:
--   1. silver_kalshi_cfb_trade_candles_1min
--   2. silver_kalshi_cfb_market_state_1min
--
-- Grain:
--   One row per Kalshi market ticker per minute.
--
-- Price convention:
--   yes_price_dollars is already represented from 0.00 to 1.00.
--
-- ============================================================


-- ============================================================
-- TABLE 1
-- SILVER ONE-MINUTE TRADE CANDLES
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_kalshi_cfb_trade_candles_1min
COMMENT 'Continuous one-minute Kalshi CFB trade candles with OHLC, volume, VWAP, trade counts, directional activity, and forward-filled inactive minutes.'
AS

WITH ranked_bronze_trades AS (
    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY trade_id
            ORDER BY
                bronze_loaded_at_utc DESC NULLS LAST,
                retrieved_at_utc DESC NULLS LAST,
                ingest_run_id DESC NULLS LAST
        ) AS trade_version_rank

    FROM databricks_realtime_optimization.cfb_analytics.bronze_kalshi_cfb_trades

    WHERE trade_id IS NOT NULL
      AND trade_created_time_utc IS NOT NULL
      AND COALESCE(trade_ticker, kalshi_ticker) IS NOT NULL
      AND yes_price_dollars IS NOT NULL
      AND contracts_traded IS NOT NULL
      AND contracts_traded > 0
),


-- Keep the newest Bronze copy of each individual Kalshi trade.
deduplicated_trades AS (
    SELECT
        trade_id,

        COALESCE(
            trade_ticker,
            kalshi_ticker
        ) AS kalshi_ticker,

        trade_created_time_utc AS trade_ts,

        DATE_TRUNC(
            'MINUTE',
            trade_created_time_utc
        ) AS minute_ts,

        CAST(yes_price_dollars AS DOUBLE) AS trade_price,
        CAST(contracts_traded AS DOUBLE) AS trade_volume,

        season,
        week,
        espn_event_id,
        game_start_utc,

        away_team,
        away_abbrev_espn,
        away_abbrev_kalshi,
        away_score,

        home_team,
        home_abbrev_espn,
        home_abbrev_kalshi,
        home_score,

        market_side,
        selection_team,
        selection_abbrev_kalshi,
        opponent_team,
        opponent_abbrev_kalshi,

        taker_side,
        taker_book_side,
        taker_outcome_side,
        taker_bet_on_selection,
        taker_bet_against_selection,
        is_block_trade

    FROM ranked_bronze_trades

    WHERE trade_version_rank = 1
),


-- Store stable game and market attributes once per Kalshi ticker.
market_metadata AS (
    SELECT
        kalshi_ticker,

        MAX(season) AS season,
        MAX(week) AS week,
        MAX(espn_event_id) AS espn_event_id,
        MAX(game_start_utc) AS game_start_utc,

        MAX(away_team) AS away_team,
        MAX(away_abbrev_espn) AS away_abbrev_espn,
        MAX(away_abbrev_kalshi) AS away_abbrev_kalshi,

        MAX(home_team) AS home_team,
        MAX(home_abbrev_espn) AS home_abbrev_espn,
        MAX(home_abbrev_kalshi) AS home_abbrev_kalshi,

        MAX(market_side) AS market_side,
        MAX(selection_team) AS selection_team,
        MAX(selection_abbrev_kalshi) AS selection_abbrev_kalshi,
        MAX(opponent_team) AS opponent_team,
        MAX(opponent_abbrev_kalshi) AS opponent_abbrev_kalshi

    FROM deduplicated_trades

    GROUP BY kalshi_ticker
),


-- Aggregate actual trades into observed one-minute candles.
observed_candles AS (
    SELECT
        kalshi_ticker,
        minute_ts,

        MIN_BY(
            trade_price,
            trade_ts
        ) AS open_price,

        MAX(trade_price) AS high_price,

        MIN(trade_price) AS low_price,

        MAX_BY(
            trade_price,
            trade_ts
        ) AS close_price,

        SUM(trade_volume) AS volume,

        SUM(trade_price * trade_volume)
            / NULLIF(SUM(trade_volume), 0.0) AS vwap,

        COUNT(*) AS trade_count,

        COUNT_IF(
            COALESCE(taker_bet_on_selection, FALSE)
        ) AS selection_buy_trade_count,

        COUNT_IF(
            COALESCE(taker_bet_against_selection, FALSE)
        ) AS selection_sell_trade_count,

        SUM(
            CASE
                WHEN COALESCE(taker_bet_on_selection, FALSE)
                THEN trade_volume
                ELSE 0.0
            END
        ) AS selection_buy_volume,

        SUM(
            CASE
                WHEN COALESCE(taker_bet_against_selection, FALSE)
                THEN trade_volume
                ELSE 0.0
            END
        ) AS selection_sell_volume,

        COUNT_IF(
            COALESCE(is_block_trade, FALSE)
        ) AS block_trade_count

    FROM deduplicated_trades

    GROUP BY
        kalshi_ticker,
        minute_ts
),


-- Determine the observed trading boundaries for each market.
market_boundaries AS (
    SELECT
        kalshi_ticker,
        MIN(minute_ts) AS first_minute_ts,
        MAX(minute_ts) AS last_minute_ts

    FROM observed_candles

    GROUP BY kalshi_ticker
),


-- Generate one row for every minute between the first and last trade.
market_minute_spine AS (
    SELECT
        boundaries.kalshi_ticker,
        generated.minute_ts

    FROM market_boundaries AS boundaries

    LATERAL VIEW EXPLODE(
        SEQUENCE(
            boundaries.first_minute_ts,
            boundaries.last_minute_ts,
            INTERVAL 1 MINUTE
        )
    ) generated AS minute_ts
),


-- Join observed candles to the continuous minute timeline.
timeline_with_gaps AS (
    SELECT
        spine.kalshi_ticker,
        spine.minute_ts,

        candles.open_price,
        candles.high_price,
        candles.low_price,
        candles.close_price,
        candles.volume,
        candles.vwap,
        candles.trade_count,

        candles.selection_buy_trade_count,
        candles.selection_sell_trade_count,
        candles.selection_buy_volume,
        candles.selection_sell_volume,
        candles.block_trade_count,

        candles.kalshi_ticker IS NOT NULL AS had_trades

    FROM market_minute_spine AS spine

    LEFT JOIN observed_candles AS candles
        ON spine.kalshi_ticker = candles.kalshi_ticker
       AND spine.minute_ts = candles.minute_ts
),


-- Carry the most recent observed close into inactive minutes.
forward_filled_timeline AS (
    SELECT
        *,

        LAST(close_price, TRUE) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_observed_close

    FROM timeline_with_gaps
)


SELECT
    timeline.kalshi_ticker,

    metadata.season,
    metadata.week,
    metadata.espn_event_id,
    metadata.game_start_utc,

    metadata.away_team,
    metadata.away_abbrev_espn,
    metadata.away_abbrev_kalshi,

    metadata.home_team,
    metadata.home_abbrev_espn,
    metadata.home_abbrev_kalshi,

    metadata.market_side,
    metadata.selection_team,
    metadata.selection_abbrev_kalshi,
    metadata.opponent_team,
    metadata.opponent_abbrev_kalshi,

    timeline.minute_ts,

    COALESCE(
        timeline.open_price,
        timeline.last_observed_close
    ) AS open_price,

    COALESCE(
        timeline.high_price,
        timeline.last_observed_close
    ) AS high_price,

    COALESCE(
        timeline.low_price,
        timeline.last_observed_close
    ) AS low_price,

    COALESCE(
        timeline.close_price,
        timeline.last_observed_close
    ) AS close_price,

    COALESCE(
        timeline.vwap,
        timeline.last_observed_close
    ) AS vwap,

    COALESCE(
        timeline.volume,
        0.0
    ) AS volume,

    COALESCE(
        timeline.trade_count,
        0
    ) AS trade_count,

    COALESCE(
        timeline.selection_buy_trade_count,
        0
    ) AS selection_buy_trade_count,

    COALESCE(
        timeline.selection_sell_trade_count,
        0
    ) AS selection_sell_trade_count,

    COALESCE(
        timeline.selection_buy_volume,
        0.0
    ) AS selection_buy_volume,

    COALESCE(
        timeline.selection_sell_volume,
        0.0
    ) AS selection_sell_volume,

    COALESCE(
        timeline.block_trade_count,
        0
    ) AS block_trade_count,

    timeline.had_trades,

    NOT timeline.had_trades AS is_inactive_minute,

    -- Price is already represented as a probability from 0.00 to 1.00.
    COALESCE(
        timeline.close_price,
        timeline.last_observed_close
    ) AS implied_yes_probability,

    -- Negative before kickoff, zero at kickoff, positive after kickoff.
    TIMESTAMPDIFF(
        MINUTE,
        metadata.game_start_utc,
        timeline.minute_ts
    ) AS minutes_from_game_start,

    CURRENT_TIMESTAMP() AS silver_updated_at_utc

FROM forward_filled_timeline AS timeline

LEFT JOIN market_metadata AS metadata
    ON timeline.kalshi_ticker = metadata.kalshi_ticker;



-- ============================================================
-- TABLE 2
-- SILVER ONE-MINUTE MARKET-STATE FEATURES
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_kalshi_cfb_market_state_1min
COMMENT 'Minute-level Kalshi CFB analytical features including returns, rolling averages, rolling volatility, momentum, volume statistics, and anomaly-detection inputs.'
AS

WITH candle_lags AS (
    SELECT
        candles.*,

        LAG(close_price, 1) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS close_lag_1m,

        LAG(close_price, 5) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS close_lag_5m,

        LAG(close_price, 15) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS close_lag_15m,

        LAG(close_price, 30) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS close_lag_30m,

        LAG(close_price, 60) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS close_lag_60m,

        LAG(volume, 1) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS volume_lag_1m,

        LAG(trade_count, 1) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
        ) AS trade_count_lag_1m

    FROM silver_kalshi_cfb_trade_candles_1min AS candles
),


-- Calculate returns and point-in-time momentum.
returns_and_momentum AS (
    SELECT
        *,

        close_price - close_lag_1m AS price_change_1m,

        CASE
            WHEN close_lag_1m > 0
            THEN (close_price / close_lag_1m) - 1.0
        END AS return_1m,

        CASE
            WHEN close_price > 0
             AND close_lag_1m > 0
            THEN LN(close_price / close_lag_1m)
        END AS log_return_1m,

        close_price - close_lag_5m AS momentum_5m,
        close_price - close_lag_15m AS momentum_15m,
        close_price - close_lag_30m AS momentum_30m,
        close_price - close_lag_60m AS momentum_60m,

        CASE
            WHEN close_lag_5m > 0
            THEN (close_price / close_lag_5m) - 1.0
        END AS return_5m,

        CASE
            WHEN close_lag_15m > 0
            THEN (close_price / close_lag_15m) - 1.0
        END AS return_15m,

        CASE
            WHEN close_lag_30m > 0
            THEN (close_price / close_lag_30m) - 1.0
        END AS return_30m,

        CASE
            WHEN close_lag_60m > 0
            THEN (close_price / close_lag_60m) - 1.0
        END AS return_60m,

        volume - volume_lag_1m AS volume_change_1m,

        trade_count - trade_count_lag_1m
            AS trade_count_change_1m,

        selection_buy_volume - selection_sell_volume
            AS selection_net_volume,

        selection_buy_trade_count - selection_sell_trade_count
            AS selection_net_trade_count,

        CASE
            WHEN volume > 0
            THEN (
                selection_buy_volume - selection_sell_volume
            ) / volume
        END AS selection_volume_imbalance

    FROM candle_lags
),


-- Calculate rolling statistics over several minute horizons.
rolling_statistics AS (
    SELECT
        *,

        -- ----------------------------------------------------
        -- Rolling closing-price averages
        -- ----------------------------------------------------

        AVG(close_price) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS close_sma_5m,

        AVG(close_price) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS close_sma_15m,

        AVG(close_price) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS close_sma_30m,

        AVG(close_price) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS close_sma_60m,


        -- ----------------------------------------------------
        -- Rolling VWAP averages
        -- ----------------------------------------------------

        AVG(vwap) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vwap_sma_5m,

        AVG(vwap) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS vwap_sma_15m,

        AVG(vwap) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS vwap_sma_60m,


        -- ----------------------------------------------------
        -- Rolling volatility of one-minute log returns
        -- ----------------------------------------------------

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volatility_5m,

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volatility_15m,

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30m,

        STDDEV_SAMP(log_return_1m) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volatility_60m,


        -- ----------------------------------------------------
        -- Rolling volume statistics
        -- ----------------------------------------------------

        AVG(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volume_avg_5m,

        SUM(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volume_sum_5m,

        AVG(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volume_avg_15m,

        STDDEV_SAMP(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volume_stddev_15m,

        SUM(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volume_sum_15m,

        AVG(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volume_avg_60m,

        STDDEV_SAMP(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volume_stddev_60m,

        SUM(volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volume_sum_60m,


        -- ----------------------------------------------------
        -- Rolling trade-count statistics
        -- ----------------------------------------------------

        AVG(trade_count) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS trade_count_avg_15m,

        STDDEV_SAMP(trade_count) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS trade_count_stddev_15m,

        SUM(trade_count) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS trade_count_sum_15m,

        SUM(trade_count) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS trade_count_sum_60m,


        -- ----------------------------------------------------
        -- Trading-activity statistics
        -- ----------------------------------------------------

        SUM(
            CASE
                WHEN had_trades THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS active_minutes_15m,

        SUM(
            CASE
                WHEN had_trades THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS active_minutes_60m,

        SUM(selection_buy_volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS selection_buy_volume_15m,

        SUM(selection_sell_volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS selection_sell_volume_15m,

        SUM(selection_net_volume) OVER (
            PARTITION BY kalshi_ticker
            ORDER BY minute_ts
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS selection_net_volume_15m

    FROM returns_and_momentum
),


-- Calculate derived deviations and anomaly-oriented features.
derived_features AS (
    SELECT
        *,

        close_price - close_sma_5m AS close_vs_sma_5m,
        close_price - close_sma_15m AS close_vs_sma_15m,
        close_price - close_sma_30m AS close_vs_sma_30m,
        close_price - close_sma_60m AS close_vs_sma_60m,

        close_price - vwap AS close_vs_vwap,
        close_price - vwap_sma_5m AS close_vs_vwap_sma_5m,
        close_price - vwap_sma_15m AS close_vs_vwap_sma_15m,
        close_price - vwap_sma_60m AS close_vs_vwap_sma_60m,

        CASE
            WHEN volume_stddev_15m > 0
            THEN (
                volume - volume_avg_15m
            ) / volume_stddev_15m
        END AS volume_zscore_15m,

        CASE
            WHEN volume_stddev_60m > 0
            THEN (
                volume - volume_avg_60m
            ) / volume_stddev_60m
        END AS volume_zscore_60m,

        CASE
            WHEN trade_count_stddev_15m > 0
            THEN (
                trade_count - trade_count_avg_15m
            ) / trade_count_stddev_15m
        END AS trade_count_zscore_15m,

        CASE
            WHEN close_sma_15m > 0
            THEN (
                close_price - close_sma_15m
            ) / close_sma_15m
        END AS close_deviation_pct_15m,

        CASE
            WHEN close_sma_60m > 0
            THEN (
                close_price - close_sma_60m
            ) / close_sma_60m
        END AS close_deviation_pct_60m,

        CASE
            WHEN selection_buy_volume_15m
               + selection_sell_volume_15m > 0
            THEN selection_net_volume_15m
                / (
                    selection_buy_volume_15m
                    + selection_sell_volume_15m
                )
        END AS selection_volume_imbalance_15m,

        CAST(active_minutes_15m AS DOUBLE) / 15.0
            AS active_minute_ratio_15m,

        CAST(active_minutes_60m AS DOUBLE) / 60.0
            AS active_minute_ratio_60m

    FROM rolling_statistics
)


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
    minutes_from_game_start,

    -- Current candle
    open_price,
    high_price,
    low_price,
    close_price,
    implied_yes_probability,
    vwap,
    volume,
    trade_count,

    had_trades,
    is_inactive_minute,

    -- Current directional trading activity
    selection_buy_trade_count,
    selection_sell_trade_count,
    selection_buy_volume,
    selection_sell_volume,
    selection_net_trade_count,
    selection_net_volume,
    selection_volume_imbalance,
    block_trade_count,

    -- Lagged values
    close_lag_1m,
    close_lag_5m,
    close_lag_15m,
    close_lag_30m,
    close_lag_60m,

    -- Returns and changes
    price_change_1m,
    return_1m,
    log_return_1m,
    return_5m,
    return_15m,
    return_30m,
    return_60m,
    volume_change_1m,
    trade_count_change_1m,

    -- Momentum
    momentum_5m,
    momentum_15m,
    momentum_30m,
    momentum_60m,

    -- Price rolling averages
    close_sma_5m,
    close_sma_15m,
    close_sma_30m,
    close_sma_60m,

    close_vs_sma_5m,
    close_vs_sma_15m,
    close_vs_sma_30m,
    close_vs_sma_60m,

    close_deviation_pct_15m,
    close_deviation_pct_60m,

    -- VWAP features
    vwap_sma_5m,
    vwap_sma_15m,
    vwap_sma_60m,

    close_vs_vwap,
    close_vs_vwap_sma_5m,
    close_vs_vwap_sma_15m,
    close_vs_vwap_sma_60m,

    -- Rolling volatility
    volatility_5m,
    volatility_15m,
    volatility_30m,
    volatility_60m,

    -- Volume statistics
    volume_avg_5m,
    volume_sum_5m,

    volume_avg_15m,
    volume_stddev_15m,
    volume_sum_15m,
    volume_zscore_15m,

    volume_avg_60m,
    volume_stddev_60m,
    volume_sum_60m,
    volume_zscore_60m,

    -- Trade-count statistics
    trade_count_avg_15m,
    trade_count_stddev_15m,
    trade_count_sum_15m,
    trade_count_sum_60m,
    trade_count_zscore_15m,

    -- Market activity statistics
    active_minutes_15m,
    active_minutes_60m,
    active_minute_ratio_15m,
    active_minute_ratio_60m,

    selection_buy_volume_15m,
    selection_sell_volume_15m,
    selection_net_volume_15m,
    selection_volume_imbalance_15m,

    CURRENT_TIMESTAMP() AS silver_updated_at_utc

FROM derived_features;