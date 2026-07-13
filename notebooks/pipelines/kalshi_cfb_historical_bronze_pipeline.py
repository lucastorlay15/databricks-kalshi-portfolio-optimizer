# ============================================================
# Kalshi CFB Historical Bronze Pipeline
#
# Purpose:
#   Convert historical raw landing JSON pages into:
#     1. bronze_kalshi_cfb_trade_pages_audit
#        - one row per API response page
#     2. bronze_kalshi_cfb_trades
#        - one row per Kalshi trade
#
# This is batch-oriented historical processing.
# No streaming / Auto Loader required yet.
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

CATALOG = "databricks_realtime_optimization"
SCHEMA = "cfb_analytics"
VOLUME = "landing"

LANDING_BASE_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/kalshi/cfb/trades"
)

LANDING_JSON_PATH = (
    f"{LANDING_BASE_PATH}/season=*/espn_event_id=*/market_side=*/ticker=*/page_*.json"
)


# ------------------------------------------------------------
# Bronze audit table
# ------------------------------------------------------------

@dp.materialized_view(
    name="bronze_kalshi_cfb_trade_pages_audit",
    comment=(
        "Audit table for historical Kalshi CFB trade landing pages. "
        "One row per landed API response page."
    )
)
def bronze_kalshi_cfb_trade_pages_audit():
    raw_pages_df = (
        spark.read
            .format("json")
            .load(LANDING_JSON_PATH)
            .withColumn("source_file_path", F.input_file_name())
    )

    return (
        raw_pages_df
        .select(
            F.col("checkpoint_run_id").cast("string").alias("checkpoint_run_id"),
            F.col("ingest_run_id").cast("string").alias("ingest_run_id"),
            F.col("retrieved_at_utc").cast("timestamp").alias("retrieved_at_utc"),

            F.col("season").cast("int").alias("season"),
            F.col("espn_event_id").cast("string").alias("espn_event_id"),
            F.col("date_utc").cast("timestamp").alias("game_start_utc"),
            F.col("week").cast("int").alias("week"),

            F.col("away_team").cast("string").alias("away_team"),
            F.col("away_abbrev_espn").cast("string").alias("away_abbrev_espn"),
            F.col("away_abbrev_kalshi").cast("string").alias("away_abbrev_kalshi"),
            F.col("away_score").cast("double").alias("away_score"),

            F.col("home_team").cast("string").alias("home_team"),
            F.col("home_abbrev_espn").cast("string").alias("home_abbrev_espn"),
            F.col("home_abbrev_kalshi").cast("string").alias("home_abbrev_kalshi"),
            F.col("home_score").cast("double").alias("home_score"),

            F.col("market_side").cast("string").alias("market_side"),
            F.col("selection_team").cast("string").alias("selection_team"),
            F.col("selection_abbrev_kalshi").cast("string").alias("selection_abbrev_kalshi"),
            F.col("opponent_team").cast("string").alias("opponent_team"),
            F.col("opponent_abbrev_kalshi").cast("string").alias("opponent_abbrev_kalshi"),

            F.col("kalshi_ticker").cast("string").alias("kalshi_ticker"),
            F.col("kalshi_endpoint_type").cast("string").alias("kalshi_endpoint_type"),

            F.col("min_ts").cast("long").alias("min_ts"),
            F.col("max_ts").cast("long").alias("max_ts"),
            F.col("limit").cast("int").alias("api_limit"),
            F.col("page_num").cast("int").alias("page_num"),

            F.col("request_url").cast("string").alias("request_url"),
            F.col("status_code").cast("int").alias("status_code"),
            F.col("trade_count_on_page").cast("int").alias("trade_count_on_page"),

            F.col("request_error").cast("string").alias("request_error"),
            F.col("json_parse_error").cast("string").alias("json_parse_error"),
            F.col("attempts").cast("int").alias("attempts"),
            F.col("retryable_failure").cast("boolean").alias("retryable_failure"),
            F.col("ticker_failed").cast("boolean").alias("ticker_failed"),
            F.col("failure_reason").cast("string").alias("failure_reason"),

            F.col("response.cursor").cast("string").alias("response_cursor"),

            F.size(F.col("response.trades")).cast("int").alias("actual_trade_array_count"),

            F.col("source_file_path").cast("string").alias("source_file_path"),

            F.current_timestamp().alias("bronze_loaded_at_utc")
        )
        .withColumn(
            "page_row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("checkpoint_run_id"), F.lit("")),
                    F.coalesce(F.col("ingest_run_id"), F.lit("")),
                    F.coalesce(F.col("season").cast("string"), F.lit("")),
                    F.coalesce(F.col("espn_event_id"), F.lit("")),
                    F.coalesce(F.col("market_side"), F.lit("")),
                    F.coalesce(F.col("kalshi_ticker"), F.lit("")),
                    F.coalesce(F.col("page_num").cast("string"), F.lit("")),
                    F.coalesce(F.col("source_file_path"), F.lit(""))
                ),
                256
            )
        )
    )


# ------------------------------------------------------------
# Bronze trade table
# ------------------------------------------------------------

@dp.materialized_view(
    name="bronze_kalshi_cfb_trades",
    comment=(
        "Historical Kalshi CFB trade-level Bronze table. "
        "One row per trade exploded from raw landing JSON pages."
    )
)
def bronze_kalshi_cfb_trades():
    raw_pages_df = (
        spark.read
            .format("json")
            .load(LANDING_JSON_PATH)
            .withColumn("source_file_path", F.input_file_name())
    )

    exploded_trades_df = (
        raw_pages_df
        .where(F.col("status_code").cast("int") == 200)
        .withColumn("trade", F.explode_outer(F.col("response.trades")))
        .where(F.col("trade").isNotNull())
    )

    return (
        exploded_trades_df
        .select(
            # Ingest / audit metadata
            F.col("checkpoint_run_id").cast("string").alias("checkpoint_run_id"),
            F.col("ingest_run_id").cast("string").alias("ingest_run_id"),
            F.col("retrieved_at_utc").cast("timestamp").alias("retrieved_at_utc"),

            # Game metadata
            F.col("season").cast("int").alias("season"),
            F.col("espn_event_id").cast("string").alias("espn_event_id"),
            F.col("date_utc").cast("timestamp").alias("game_start_utc"),
            F.col("week").cast("int").alias("week"),

            F.col("away_team").cast("string").alias("away_team"),
            F.col("away_abbrev_espn").cast("string").alias("away_abbrev_espn"),
            F.col("away_abbrev_kalshi").cast("string").alias("away_abbrev_kalshi"),
            F.col("away_score").cast("double").alias("away_score"),

            F.col("home_team").cast("string").alias("home_team"),
            F.col("home_abbrev_espn").cast("string").alias("home_abbrev_espn"),
            F.col("home_abbrev_kalshi").cast("string").alias("home_abbrev_kalshi"),
            F.col("home_score").cast("double").alias("home_score"),

            # Market metadata
            F.col("market_side").cast("string").alias("market_side"),
            F.col("selection_team").cast("string").alias("selection_team"),
            F.col("selection_abbrev_kalshi").cast("string").alias("selection_abbrev_kalshi"),
            F.col("opponent_team").cast("string").alias("opponent_team"),
            F.col("opponent_abbrev_kalshi").cast("string").alias("opponent_abbrev_kalshi"),

            F.col("kalshi_ticker").cast("string").alias("kalshi_ticker"),
            F.col("kalshi_endpoint_type").cast("string").alias("kalshi_endpoint_type"),

            # Page metadata
            F.col("min_ts").cast("long").alias("min_ts"),
            F.col("max_ts").cast("long").alias("max_ts"),
            F.col("page_num").cast("int").alias("page_num"),
            F.col("request_url").cast("string").alias("request_url"),
            F.col("source_file_path").cast("string").alias("source_file_path"),

            # Trade fields
            F.col("trade.trade_id").cast("string").alias("trade_id"),
            F.to_timestamp(F.col("trade.created_time")).alias("trade_created_time_utc"),

            F.col("trade.ticker").cast("string").alias("trade_ticker"),

            F.col("trade.count_fp").cast("double").alias("contracts_traded"),

            F.col("trade.yes_price_dollars").cast("double").alias("yes_price_dollars"),
            F.col("trade.no_price_dollars").cast("double").alias("no_price_dollars"),

            F.col("trade.taker_side").cast("string").alias("taker_side"),
            F.col("trade.taker_book_side").cast("string").alias("taker_book_side"),
            F.col("trade.taker_outcome_side").cast("string").alias("taker_outcome_side"),

            F.col("trade.is_block_trade").cast("boolean").alias("is_block_trade"),

            F.current_timestamp().alias("bronze_loaded_at_utc")
        )
        .withColumn(
            "taker_bet_on_selection",
            F.when(F.col("taker_side") == "yes", F.lit(True))
             .when(F.col("taker_side") == "no", F.lit(False))
             .otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "taker_bet_against_selection",
            F.when(F.col("taker_side") == "no", F.lit(True))
             .when(F.col("taker_side") == "yes", F.lit(False))
             .otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "implied_yes_probability",
            F.col("yes_price_dollars")
        )
        .withColumn(
            "notional_yes_dollars",
            F.col("contracts_traded") * F.col("yes_price_dollars")
        )
        .withColumn(
            "notional_no_dollars",
            F.col("contracts_traded") * F.col("no_price_dollars")
        )
        .withColumn(
            "trade_row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("trade_id"), F.lit("")),
                    F.coalesce(F.col("kalshi_ticker"), F.lit("")),
                    F.coalesce(F.col("market_side"), F.lit("")),
                    F.coalesce(F.col("trade_created_time_utc").cast("string"), F.lit("")),
                    F.coalesce(F.col("contracts_traded").cast("string"), F.lit("")),
                    F.coalesce(F.col("yes_price_dollars").cast("string"), F.lit("")),
                    F.coalesce(F.col("no_price_dollars").cast("string"), F.lit("")),
                    F.coalesce(F.col("source_file_path"), F.lit(""))
                ),
                256
            )
        )
        .where(F.col("trade_id").isNotNull())
        .where(F.col("trade_created_time_utc").isNotNull())
        .where(F.col("contracts_traded") > 0)
        .where(F.col("yes_price_dollars").between(0.0, 1.0))
        .where(F.col("no_price_dollars").between(0.0, 1.0))
    )