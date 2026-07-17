from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from src.portfolio.accounting import (
    calculate_accounting_balance_difference,
    calculate_order_notional,
    quantize_money,
)
from src.portfolio.models import (
    AccountState,
    OrderSide,
    OrderState,
    OrderStatus,
    PortfolioEvent,
    PortfolioEventType,
    PortfolioSnapshot,
    PositionState,
)
from src.portfolio.portfolio_config import (
    ACCOUNTING_TOLERANCE,
    ALLOW_NEGATIVE_CASH,
    ALLOW_SHORT_POSITIONS,
    CATALOG_NAME,
    CONTRACT_PAYOUT,
    ENFORCE_ONE_ACTIVE_TICKER_PER_EVENT,
    MAX_CONTRACT_PRICE,
    MIN_CONTRACT_PRICE,
    MONEY_QUANTUM,
    PORTFOLIO_ACCOUNT_CURRENT_TABLE,
    PORTFOLIO_ARTIFACT_VOLUME_PATH,
    PORTFOLIO_EVENTS_TABLE,
    PORTFOLIO_ID,
    PORTFOLIO_ORDERS_CURRENT_TABLE,
    PORTFOLIO_POSITIONS_CURRENT_TABLE,
    PORTFOLIO_SCHEMA_VERSION,
    PORTFOLIO_VALIDATION_ARTIFACT_PATH,
    PORTFOLIO_VALIDATION_ARTIFACT_VERSION,
    REQUIRE_ORDER_FOR_FILL,
    REQUIRE_STRICT_EVENT_SEQUENCE,
    REQUIRED_PORTFOLIO_INVARIANTS,
    SCHEMA_NAME,
    STARTING_CASH,
    VALIDATE_AFTER_EACH_EVENT,
)


@dataclass(frozen=True)
class ValidationResult:
    validation_name: str
    status: str
    expected_result: str
    actual_result: str
    error_message: str | None


CASH_EVENT_TYPES = {
    PortfolioEventType.CASH_DEPOSIT,
    PortfolioEventType.CASH_WITHDRAWAL,
}

TRADE_EVENT_TYPES = {
    PortfolioEventType.ORDER_PLACED,
    PortfolioEventType.BUY_FILL,
    PortfolioEventType.SELL_FILL,
}


def _is_utc_datetime(
    value: datetime,
) -> bool:
    return (
        value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )


def _validate_trade_price(
    price: Decimal,
) -> None:
    if (
        price < MIN_CONTRACT_PRICE
        or price > MAX_CONTRACT_PRICE
    ):
        raise ValueError(
            "Trade price must be between "
            f"{MIN_CONTRACT_PRICE} and "
            f"{MAX_CONTRACT_PRICE}."
        )


def validate_portfolio_configuration() -> None:
    if not PORTFOLIO_ID:
        raise ValueError(
            "PORTFOLIO_ID must be configured."
        )

    if not PORTFOLIO_SCHEMA_VERSION:
        raise ValueError(
            "PORTFOLIO_SCHEMA_VERSION must be configured."
        )

    if STARTING_CASH <= 0:
        raise ValueError(
            "STARTING_CASH must be positive."
        )

    if CONTRACT_PAYOUT <= 0:
        raise ValueError(
            "CONTRACT_PAYOUT must be positive."
        )

    if MIN_CONTRACT_PRICE <= 0:
        raise ValueError(
            "MIN_CONTRACT_PRICE must be positive."
        )

    if MAX_CONTRACT_PRICE >= CONTRACT_PAYOUT:
        raise ValueError(
            "MAX_CONTRACT_PRICE must be less than "
            "CONTRACT_PAYOUT."
        )

    if MIN_CONTRACT_PRICE >= MAX_CONTRACT_PRICE:
        raise ValueError(
            "MIN_CONTRACT_PRICE must be less than "
            "MAX_CONTRACT_PRICE."
        )

    if MONEY_QUANTUM <= 0:
        raise ValueError(
            "MONEY_QUANTUM must be positive."
        )

    if ACCOUNTING_TOLERANCE < MONEY_QUANTUM:
        raise ValueError(
            "ACCOUNTING_TOLERANCE must be greater than "
            "or equal to MONEY_QUANTUM."
        )

    configured_identifiers = [
        CATALOG_NAME,
        SCHEMA_NAME,
        PORTFOLIO_EVENTS_TABLE,
        PORTFOLIO_ACCOUNT_CURRENT_TABLE,
        PORTFOLIO_POSITIONS_CURRENT_TABLE,
        PORTFOLIO_ORDERS_CURRENT_TABLE,
        PORTFOLIO_ARTIFACT_VOLUME_PATH,
        PORTFOLIO_VALIDATION_ARTIFACT_PATH,
    ]

    if any(
        not configured_identifier
        for configured_identifier in configured_identifiers
    ):
        raise ValueError(
            "All portfolio table and artifact identifiers "
            "must be configured."
        )


def validate_event(
    event: PortfolioEvent,
) -> None:
    if not event.event_id:
        raise ValueError(
            "Portfolio event_id must be populated."
        )

    if event.event_sequence <= 0:
        raise ValueError(
            "Portfolio event_sequence must be positive."
        )

    if not event.portfolio_id:
        raise ValueError(
            "Portfolio event portfolio_id must be populated."
        )

    if not _is_utc_datetime(
        event.event_ts
    ):
        raise ValueError(
            "Portfolio event timestamps must be "
            "timezone-aware UTC datetimes."
        )

    if event.fee < 0:
        raise ValueError(
            "Portfolio event fees cannot be negative."
        )

    if event.event_type in CASH_EVENT_TYPES:
        if (
            event.cash_amount is None
            or event.cash_amount <= 0
        ):
            raise ValueError(
                "Cash events require a positive cash_amount."
            )

        return

    if event.event_type == PortfolioEventType.ORDER_CANCELLED:
        if not event.order_id:
            raise ValueError(
                "Order cancellation events require order_id."
            )

        return

    if not event.ticker:
        raise ValueError(
            "Trading and settlement events require ticker."
        )

    if not event.espn_event_id:
        raise ValueError(
            "Trading and settlement events require "
            "espn_event_id."
        )

    if event.event_type == PortfolioEventType.ORDER_PLACED:
        if not event.order_id:
            raise ValueError(
                "Order placement events require order_id."
            )

        if event.side is None:
            raise ValueError(
                "Order placement events require side."
            )

        if event.quantity <= 0:
            raise ValueError(
                "Order placement quantity must be positive."
            )

        if event.price is None:
            raise ValueError(
                "Order placement events require limit price."
            )

        _validate_trade_price(
            event.price
        )

        return

    if event.event_type in {
        PortfolioEventType.BUY_FILL,
        PortfolioEventType.SELL_FILL,
    }:
        if REQUIRE_ORDER_FOR_FILL and not event.order_id:
            raise ValueError(
                "Fill events require order_id under the "
                "current portfolio configuration."
            )

        expected_side = (
            OrderSide.BUY
            if event.event_type == PortfolioEventType.BUY_FILL
            else OrderSide.SELL
        )

        if event.side != expected_side:
            raise ValueError(
                "Fill event side does not match event type."
            )

        if event.quantity <= 0:
            raise ValueError(
                "Fill event quantity must be positive."
            )

        if event.price is None:
            raise ValueError(
                "Fill events require execution price."
            )

        _validate_trade_price(
            event.price
        )

        return

    if event.event_type == PortfolioEventType.SETTLEMENT:
        if event.quantity <= 0:
            raise ValueError(
                "Settlement quantity must be positive."
            )

        if event.price not in {
            Decimal("0"),
            CONTRACT_PAYOUT,
        }:
            raise ValueError(
                "Settlement price must be zero or the "
                "configured contract payout."
            )

        return

    raise ValueError(
        f"Unsupported portfolio event type: "
        f"{event.event_type!r}"
    )


def validate_event_sequence(
    events: Iterable[PortfolioEvent],
    initial_event_sequence: int = 0,
    initial_event_ts: datetime | None = None,
) -> None:
    event_list = list(
        events
    )

    event_ids: set[str] = set()
    event_sequences: set[int] = set()
    previous_event_sequence = (
        initial_event_sequence
    )
    previous_event_ts = (
        initial_event_ts
    )
    portfolio_id: str | None = None

    for event in event_list:
        validate_event(
            event
        )

        if event.event_id in event_ids:
            raise ValueError(
                "Portfolio event IDs must be unique. "
                f"Duplicate event_id: {event.event_id}."
            )

        if event.event_sequence in event_sequences:
            raise ValueError(
                "Portfolio event sequences must be unique. "
                f"Duplicate sequence: {event.event_sequence}."
            )

        if (
            REQUIRE_STRICT_EVENT_SEQUENCE
            and event.event_sequence
            <= previous_event_sequence
        ):
            raise ValueError(
                "Portfolio event sequences must be strictly "
                "increasing."
            )

        if (
            previous_event_ts is not None
            and event.event_ts < previous_event_ts
        ):
            raise ValueError(
                "Portfolio event timestamps cannot move "
                "backward as sequence numbers increase."
            )

        if portfolio_id is None:
            portfolio_id = event.portfolio_id
        elif event.portfolio_id != portfolio_id:
            raise ValueError(
                "A replay batch cannot contain events from "
                "multiple portfolios."
            )

        event_ids.add(
            event.event_id
        )
        event_sequences.add(
            event.event_sequence
        )
        previous_event_sequence = (
            event.event_sequence
        )
        previous_event_ts = (
            event.event_ts
        )


def validate_account_state(
    account: AccountState,
) -> None:
    if not account.portfolio_id:
        raise ValueError(
            "Account portfolio_id must be populated."
        )

    if (
        not ALLOW_NEGATIVE_CASH
        and account.available_cash < 0
    ):
        raise ValueError(
            "Available cash cannot be negative."
        )

    if account.reserved_cash < 0:
        raise ValueError(
            "Reserved cash cannot be negative."
        )

    if account.total_deposits < 0:
        raise ValueError(
            "Total deposits cannot be negative."
        )

    if account.total_withdrawals < 0:
        raise ValueError(
            "Total withdrawals cannot be negative."
        )

    if account.fees_paid < 0:
        raise ValueError(
            "Account fees paid cannot be negative."
        )

    if not _is_utc_datetime(
        account.updated_at
    ):
        raise ValueError(
            "Account updated_at must be a UTC datetime."
        )


def validate_order_state(
    order: OrderState,
) -> None:
    if not order.order_id:
        raise ValueError(
            "Order ID must be populated."
        )

    if order.requested_quantity <= 0:
        raise ValueError(
            "Order requested quantity must be positive."
        )

    if order.filled_quantity < 0:
        raise ValueError(
            "Order filled quantity cannot be negative."
        )

    if order.filled_quantity > order.requested_quantity:
        raise ValueError(
            "Order filled quantity cannot exceed requested "
            "quantity."
        )

    _validate_trade_price(
        order.limit_price
    )

    if order.reserved_cash < 0:
        raise ValueError(
            "Order reserved cash cannot be negative."
        )

    if (
        order.side == OrderSide.BUY
        and order.is_active
    ):
        expected_reserved_cash = (
            calculate_order_notional(
                quantity=order.remaining_quantity,
                price=order.limit_price,
            )
        )

        if (
            quantize_money(
                order.reserved_cash
            )
            != expected_reserved_cash
        ):
            raise ValueError(
                "Active buy-order reserved cash does not "
                "match remaining order notional."
            )

    if (
        order.side == OrderSide.SELL
        and order.reserved_cash != 0
    ):
        raise ValueError(
            "Sell orders cannot reserve cash."
        )

    if (
        not order.is_active
        and order.reserved_cash != 0
    ):
        raise ValueError(
            "Inactive orders cannot retain reserved cash."
        )

    if not _is_utc_datetime(
        order.created_at
    ):
        raise ValueError(
            "Order created_at must be a UTC datetime."
        )

    if not _is_utc_datetime(
        order.updated_at
    ):
        raise ValueError(
            "Order updated_at must be a UTC datetime."
        )


def validate_position_state(
    position: PositionState,
) -> None:
    if not position.ticker:
        raise ValueError(
            "Position ticker must be populated."
        )

    if not position.espn_event_id:
        raise ValueError(
            "Position ESPN event ID must be populated."
        )

    if (
        not ALLOW_SHORT_POSITIONS
        and position.quantity <= 0
    ):
        raise ValueError(
            "Position quantity must be positive."
        )

    if position.average_entry_price <= 0:
        raise ValueError(
            "Average entry price must be positive."
        )

    if position.cost_basis < 0:
        raise ValueError(
            "Position cost basis cannot be negative."
        )

    if (
        position.current_price < 0
        or position.current_price > CONTRACT_PAYOUT
    ):
        raise ValueError(
            "Current position price must be between zero "
            "and the configured contract payout."
        )

    if position.fees_paid < 0:
        raise ValueError(
            "Position fees paid cannot be negative."
        )

    datetime_values = [
        position.opened_at,
        position.last_increased_at,
        position.last_updated_at,
    ]

    if position.last_reduced_at is not None:
        datetime_values.append(
            position.last_reduced_at
        )

    if any(
        not _is_utc_datetime(
            datetime_value
        )
        for datetime_value in datetime_values
    ):
        raise ValueError(
            "Position timestamps must be UTC datetimes."
        )


def validate_one_active_ticker_per_event(
    snapshot: PortfolioSnapshot,
) -> None:
    if not ENFORCE_ONE_ACTIVE_TICKER_PER_EVENT:
        return

    active_tickers_by_event: dict[str, set[str]] = {}

    for position in snapshot.positions:
        active_tickers_by_event.setdefault(
            position.espn_event_id,
            set(),
        ).add(
            position.ticker
        )

    for order in snapshot.orders:
        if (
            order.is_active
            and order.side == OrderSide.BUY
        ):
            active_tickers_by_event.setdefault(
                order.espn_event_id,
                set(),
            ).add(
                order.ticker
            )

    conflicting_events = {
        espn_event_id: sorted(
            tickers
        )
        for espn_event_id, tickers
        in active_tickers_by_event.items()
        if len(tickers) > 1
    }

    if conflicting_events:
        raise ValueError(
            "Only one active Kalshi ticker may be owned or "
            "pending purchase for each ESPN event. "
            f"Conflicts: {conflicting_events}"
        )


def validate_accounting_identity(
    snapshot: PortfolioSnapshot,
) -> None:
    balance_difference = (
        calculate_accounting_balance_difference(
            account=snapshot.account,
            positions=snapshot.positions,
        )
    )

    if abs(
        balance_difference
    ) > ACCOUNTING_TOLERANCE:
        raise ValueError(
            "Portfolio accounting identity is out of "
            "balance. "
            f"Difference: {balance_difference}."
        )


def validate_snapshot(
    snapshot: PortfolioSnapshot,
) -> None:
    if not snapshot.portfolio_id:
        raise ValueError(
            "Snapshot portfolio_id must be populated."
        )

    if snapshot.schema_version != PORTFOLIO_SCHEMA_VERSION:
        raise ValueError(
            "Snapshot schema version does not match the "
            "current portfolio configuration."
        )

    if not _is_utc_datetime(
        snapshot.as_of_ts
    ):
        raise ValueError(
            "Snapshot as_of_ts must be a UTC datetime."
        )

    if snapshot.last_event_sequence < 0:
        raise ValueError(
            "Snapshot last_event_sequence cannot be negative."
        )

    if snapshot.account.portfolio_id != snapshot.portfolio_id:
        raise ValueError(
            "Account portfolio_id does not match snapshot."
        )

    validate_account_state(
        snapshot.account
    )

    position_tickers: set[str] = set()

    for position in snapshot.positions:
        if position.portfolio_id != snapshot.portfolio_id:
            raise ValueError(
                "Position portfolio_id does not match snapshot."
            )

        if position.ticker in position_tickers:
            raise ValueError(
                "Snapshot positions must have unique tickers."
            )

        position_tickers.add(
            position.ticker
        )
        validate_position_state(
            position
        )

    order_ids: set[str] = set()

    for order in snapshot.orders:
        if order.portfolio_id != snapshot.portfolio_id:
            raise ValueError(
                "Order portfolio_id does not match snapshot."
            )

        if order.order_id in order_ids:
            raise ValueError(
                "Snapshot orders must have unique order IDs."
            )

        order_ids.add(
            order.order_id
        )
        validate_order_state(
            order
        )

    validate_one_active_ticker_per_event(
        snapshot
    )
    validate_accounting_identity(
        snapshot
    )


def evaluate_validation_case(
    validation_name: str,
    validation_callable: Callable[[], object],
    expected_error: bool = False,
) -> ValidationResult:
    expected_result = (
        "raises an exception"
        if expected_error
        else "completes successfully"
    )

    try:
        validation_output = (
            validation_callable()
        )
    except Exception as error:
        if expected_error:
            return ValidationResult(
                validation_name=validation_name,
                status="passed",
                expected_result=expected_result,
                actual_result=(
                    f"raised {type(error).__name__}"
                ),
                error_message=str(error),
            )

        return ValidationResult(
            validation_name=validation_name,
            status="failed",
            expected_result=expected_result,
            actual_result=(
                f"raised {type(error).__name__}"
            ),
            error_message=str(error),
        )

    if expected_error:
        return ValidationResult(
            validation_name=validation_name,
            status="failed",
            expected_result=expected_result,
            actual_result="completed successfully",
            error_message=(
                "Expected an exception, but no exception "
                "was raised."
            ),
        )

    if (
        isinstance(
            validation_output,
            bool,
        )
        and not validation_output
    ):
        return ValidationResult(
            validation_name=validation_name,
            status="failed",
            expected_result=expected_result,
            actual_result="returned False",
            error_message=(
                "Validation callable returned False."
            ),
        )

    return ValidationResult(
        validation_name=validation_name,
        status="passed",
        expected_result=expected_result,
        actual_result="completed successfully",
        error_message=None,
    )


def validation_results_to_records(
    validation_results: Iterable[ValidationResult],
) -> list[dict[str, str | None]]:
    return [
        {
            "validation_name": result.validation_name,
            "status": result.status,
            "expected_result": result.expected_result,
            "actual_result": result.actual_result,
            "error_message": result.error_message,
        }
        for result in validation_results
    ]


def raise_for_failed_validations(
    validation_results: Iterable[ValidationResult],
) -> None:
    failed_results = [
        result
        for result in validation_results
        if result.status != "passed"
    ]

    if failed_results:
        failed_names = [
            result.validation_name
            for result in failed_results
        ]

        raise AssertionError(
            "Portfolio validation suite failed. "
            f"Failed validations: {failed_names}"
        )


def build_portfolio_configuration_snapshot() -> dict:
    return {
        "portfolio_id": PORTFOLIO_ID,
        "portfolio_schema_version": (
            PORTFOLIO_SCHEMA_VERSION
        ),
        "validation_artifact_version": (
            PORTFOLIO_VALIDATION_ARTIFACT_VERSION
        ),
        "starting_cash": str(
            STARTING_CASH
        ),
        "contract_payout": str(
            CONTRACT_PAYOUT
        ),
        "minimum_contract_price": str(
            MIN_CONTRACT_PRICE
        ),
        "maximum_contract_price": str(
            MAX_CONTRACT_PRICE
        ),
        "money_quantum": str(
            MONEY_QUANTUM
        ),
        "accounting_tolerance": str(
            ACCOUNTING_TOLERANCE
        ),
        "allow_negative_cash": (
            ALLOW_NEGATIVE_CASH
        ),
        "allow_short_positions": (
            ALLOW_SHORT_POSITIONS
        ),
        "require_order_for_fill": (
            REQUIRE_ORDER_FOR_FILL
        ),
        "enforce_one_active_ticker_per_event": (
            ENFORCE_ONE_ACTIVE_TICKER_PER_EVENT
        ),
        "require_strict_event_sequence": (
            REQUIRE_STRICT_EVENT_SEQUENCE
        ),
        "validate_after_each_event": (
            VALIDATE_AFTER_EACH_EVENT
        ),
        "portfolio_events_table": (
            PORTFOLIO_EVENTS_TABLE
        ),
        "portfolio_account_current_table": (
            PORTFOLIO_ACCOUNT_CURRENT_TABLE
        ),
        "portfolio_positions_current_table": (
            PORTFOLIO_POSITIONS_CURRENT_TABLE
        ),
        "portfolio_orders_current_table": (
            PORTFOLIO_ORDERS_CURRENT_TABLE
        ),
        "validation_artifact_path": (
            PORTFOLIO_VALIDATION_ARTIFACT_PATH
        ),
        "required_invariants": list(
            REQUIRED_PORTFOLIO_INVARIANTS
        ),
    }
