from decimal import Decimal


# ============================================================
# Portfolio identity and schema version
# ============================================================

PORTFOLIO_ID = "kalshi_cfb_primary"
PORTFOLIO_SCHEMA_VERSION = "1.0.0"
PORTFOLIO_VALIDATION_ARTIFACT_VERSION = "1.0.0"

# ============================================================
# Portfolio accounting configuration
# ============================================================

STARTING_CASH = Decimal("1000.00")
CONTRACT_PAYOUT = Decimal("1.00")
MIN_CONTRACT_PRICE = Decimal("0.01")
MAX_CONTRACT_PRICE = Decimal("0.99")
MONEY_QUANTUM = Decimal("0.0001")
ACCOUNTING_TOLERANCE = Decimal("0.0001")

ALLOW_NEGATIVE_CASH = False
ALLOW_SHORT_POSITIONS = False
REQUIRE_ORDER_FOR_FILL = True
ENFORCE_ONE_ACTIVE_TICKER_PER_EVENT = True
REQUIRE_STRICT_EVENT_SEQUENCE = True
VALIDATE_AFTER_EACH_EVENT = True

# ============================================================
# Unity Catalog persistence
# ============================================================

CATALOG_NAME = "databricks_realtime_optimization"
SCHEMA_NAME = "cfb_analytics"

PORTFOLIO_EVENTS_TABLE = (
    f"{CATALOG_NAME}.{SCHEMA_NAME}."
    "portfolio_events"
)

PORTFOLIO_ACCOUNT_CURRENT_TABLE = (
    f"{CATALOG_NAME}.{SCHEMA_NAME}."
    "portfolio_account_current"
)

PORTFOLIO_POSITIONS_CURRENT_TABLE = (
    f"{CATALOG_NAME}.{SCHEMA_NAME}."
    "portfolio_positions_current"
)

PORTFOLIO_ORDERS_CURRENT_TABLE = (
    f"{CATALOG_NAME}.{SCHEMA_NAME}."
    "portfolio_orders_current"
)

# ============================================================
# Validation artifact
# ============================================================

PORTFOLIO_ARTIFACT_VOLUME_PATH = (
    "/Volumes/"
    f"{CATALOG_NAME}/"
    f"{SCHEMA_NAME}/"
    "model_artifacts/"
    "portfolio_state_engine"
)

PORTFOLIO_VALIDATION_ARTIFACT_PATH = (
    f"{PORTFOLIO_ARTIFACT_VOLUME_PATH}/"
    "portfolio_state_validation.json"
)

REQUIRED_PORTFOLIO_INVARIANTS = [
    "nonnegative_available_cash",
    "nonnegative_reserved_cash",
    "nonnegative_position_quantity",
    "unique_event_ids",
    "strict_event_sequence",
    "accounting_identity",
    "one_active_ticker_per_espn_event",
]
