from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from src.portfolio.accounting import (
    ZERO_MONEY,
    calculate_cost_basis_allocation,
    calculate_order_notional,
    calculate_realized_pnl,
    calculate_weighted_average_entry_price,
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
    ALLOW_NEGATIVE_CASH,
    CONTRACT_PAYOUT,
    PORTFOLIO_SCHEMA_VERSION,
    REQUIRE_ORDER_FOR_FILL,
    VALIDATE_AFTER_EACH_EVENT,
)
from src.portfolio.validation import (
    validate_event,
    validate_event_sequence,
    validate_snapshot,
)


def create_empty_snapshot(
    portfolio_id: str,
    as_of_ts: datetime,
) -> PortfolioSnapshot:
    account = AccountState(
        portfolio_id=portfolio_id,
        available_cash=ZERO_MONEY,
        reserved_cash=ZERO_MONEY,
        total_deposits=ZERO_MONEY,
        total_withdrawals=ZERO_MONEY,
        realized_pnl=ZERO_MONEY,
        fees_paid=ZERO_MONEY,
        updated_at=as_of_ts,
    )

    snapshot = PortfolioSnapshot(
        portfolio_id=portfolio_id,
        schema_version=(
            PORTFOLIO_SCHEMA_VERSION
        ),
        as_of_ts=as_of_ts,
        last_event_sequence=0,
        account=account,
    )

    validate_snapshot(
        snapshot
    )

    return snapshot


def _find_order(
    snapshot: PortfolioSnapshot,
    order_id: str,
) -> OrderState:
    matching_orders = [
        order
        for order in snapshot.orders
        if order.order_id == order_id
    ]

    if not matching_orders:
        raise ValueError(
            f"Order not found: {order_id}."
        )

    return matching_orders[0]


def _find_position(
    snapshot: PortfolioSnapshot,
    ticker: str,
) -> PositionState:
    matching_positions = [
        position
        for position in snapshot.positions
        if position.ticker == ticker
    ]

    if not matching_positions:
        raise ValueError(
            f"Position not found for ticker: {ticker}."
        )

    return matching_positions[0]


def _upsert_order(
    snapshot: PortfolioSnapshot,
    updated_order: OrderState,
) -> PortfolioSnapshot:
    remaining_orders = [
        order
        for order in snapshot.orders
        if order.order_id != updated_order.order_id
    ]

    return replace(
        snapshot,
        orders=tuple(
            sorted(
                [
                    *remaining_orders,
                    updated_order,
                ],
                key=lambda order: order.order_id,
            )
        ),
    )


def _upsert_position(
    snapshot: PortfolioSnapshot,
    updated_position: PositionState,
) -> PortfolioSnapshot:
    remaining_positions = [
        position
        for position in snapshot.positions
        if position.ticker != updated_position.ticker
    ]

    return replace(
        snapshot,
        positions=tuple(
            sorted(
                [
                    *remaining_positions,
                    updated_position,
                ],
                key=lambda position: position.ticker,
            )
        ),
    )


def _remove_position(
    snapshot: PortfolioSnapshot,
    ticker: str,
) -> PortfolioSnapshot:
    return replace(
        snapshot,
        positions=tuple(
            position
            for position in snapshot.positions
            if position.ticker != ticker
        ),
    )


def _validate_available_cash(
    available_cash: Decimal,
) -> None:
    if (
        not ALLOW_NEGATIVE_CASH
        and available_cash < 0
    ):
        raise ValueError(
            "Portfolio does not have enough available cash "
            "for this transition."
        )


def _active_sell_quantity(
    snapshot: PortfolioSnapshot,
    ticker: str,
) -> int:
    return sum(
        order.remaining_quantity
        for order in snapshot.orders
        if (
            order.ticker == ticker
            and order.side == OrderSide.SELL
            and order.is_active
        )
    )


def _apply_cash_deposit(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    cash_amount = quantize_money(
        event.cash_amount
    )

    account = replace(
        snapshot.account,
        available_cash=quantize_money(
            snapshot.account.available_cash
            + cash_amount
        ),
        total_deposits=quantize_money(
            snapshot.account.total_deposits
            + cash_amount
        ),
        updated_at=event.event_ts,
    )

    return replace(
        snapshot,
        account=account,
    )


def _apply_cash_withdrawal(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    cash_amount = quantize_money(
        event.cash_amount
    )
    available_cash = quantize_money(
        snapshot.account.available_cash
        - cash_amount
    )

    _validate_available_cash(
        available_cash
    )

    account = replace(
        snapshot.account,
        available_cash=available_cash,
        total_withdrawals=quantize_money(
            snapshot.account.total_withdrawals
            + cash_amount
        ),
        updated_at=event.event_ts,
    )

    return replace(
        snapshot,
        account=account,
    )


def _apply_order_placed(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    if any(
        order.order_id == event.order_id
        for order in snapshot.orders
    ):
        raise ValueError(
            f"Order already exists: {event.order_id}."
        )

    reserved_cash = ZERO_MONEY
    account = snapshot.account

    if event.side == OrderSide.BUY:
        reserved_cash = (
            calculate_order_notional(
                quantity=event.quantity,
                price=event.price,
            )
        )
        available_cash = quantize_money(
            account.available_cash
            - reserved_cash
        )

        _validate_available_cash(
            available_cash
        )

        account = replace(
            account,
            available_cash=available_cash,
            reserved_cash=quantize_money(
                account.reserved_cash
                + reserved_cash
            ),
            updated_at=event.event_ts,
        )

    if event.side == OrderSide.SELL:
        position = _find_position(
            snapshot=snapshot,
            ticker=event.ticker,
        )
        available_quantity = (
            position.quantity
            - _active_sell_quantity(
                snapshot=snapshot,
                ticker=event.ticker,
            )
        )

        if event.quantity > available_quantity:
            raise ValueError(
                "Sell order quantity exceeds unreserved "
                "position quantity."
            )

        account = replace(
            account,
            updated_at=event.event_ts,
        )

    order = OrderState(
        order_id=event.order_id,
        portfolio_id=snapshot.portfolio_id,
        ticker=event.ticker,
        espn_event_id=event.espn_event_id,
        side=event.side,
        requested_quantity=event.quantity,
        filled_quantity=0,
        limit_price=quantize_money(
            event.price
        ),
        reserved_cash=reserved_cash,
        status=OrderStatus.OPEN,
        created_at=event.event_ts,
        updated_at=event.event_ts,
        decision_id=event.decision_id,
    )

    updated_snapshot = replace(
        snapshot,
        account=account,
    )

    return _upsert_order(
        snapshot=updated_snapshot,
        updated_order=order,
    )


def _apply_order_cancelled(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    order = _find_order(
        snapshot=snapshot,
        order_id=event.order_id,
    )

    if not order.is_active:
        raise ValueError(
            "Only open or partially filled orders can be "
            "cancelled."
        )

    account = snapshot.account

    if order.side == OrderSide.BUY:
        account = replace(
            account,
            available_cash=quantize_money(
                account.available_cash
                + order.reserved_cash
            ),
            reserved_cash=quantize_money(
                account.reserved_cash
                - order.reserved_cash
            ),
            updated_at=event.event_ts,
        )
    else:
        account = replace(
            account,
            updated_at=event.event_ts,
        )

    cancelled_order = replace(
        order,
        reserved_cash=ZERO_MONEY,
        status=OrderStatus.CANCELLED,
        updated_at=event.event_ts,
    )

    updated_snapshot = replace(
        snapshot,
        account=account,
    )

    return _upsert_order(
        snapshot=updated_snapshot,
        updated_order=cancelled_order,
    )


def _get_fill_order(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> OrderState | None:
    if event.order_id is None:
        if REQUIRE_ORDER_FOR_FILL:
            raise ValueError(
                "Fill event is missing required order_id."
            )

        return None

    order = _find_order(
        snapshot=snapshot,
        order_id=event.order_id,
    )

    if not order.is_active:
        raise ValueError(
            "Fill can only be applied to an active order."
        )

    if order.ticker != event.ticker:
        raise ValueError(
            "Fill ticker does not match order ticker."
        )

    if order.espn_event_id != event.espn_event_id:
        raise ValueError(
            "Fill ESPN event ID does not match order."
        )

    if order.side != event.side:
        raise ValueError(
            "Fill side does not match order side."
        )

    if event.quantity > order.remaining_quantity:
        raise ValueError(
            "Fill quantity exceeds remaining order quantity."
        )

    if (
        order.side == OrderSide.BUY
        and event.price > order.limit_price
    ):
        raise ValueError(
            "Buy fill price exceeds the order limit price."
        )

    if (
        order.side == OrderSide.SELL
        and event.price < order.limit_price
    ):
        raise ValueError(
            "Sell fill price is below the order limit price."
        )

    return order


def _update_order_after_fill(
    order: OrderState,
    filled_quantity: int,
    event_ts: datetime,
) -> OrderState:
    cumulative_filled_quantity = (
        order.filled_quantity
        + filled_quantity
    )

    status = (
        OrderStatus.FILLED
        if cumulative_filled_quantity
        == order.requested_quantity
        else OrderStatus.PARTIALLY_FILLED
    )

    remaining_reserved_cash = (
        calculate_order_notional(
            quantity=(
                order.requested_quantity
                - cumulative_filled_quantity
            ),
            price=order.limit_price,
        )
        if order.side == OrderSide.BUY
        else ZERO_MONEY
    )

    return replace(
        order,
        filled_quantity=(
            cumulative_filled_quantity
        ),
        reserved_cash=(
            remaining_reserved_cash
        ),
        status=status,
        updated_at=event_ts,
    )


def _apply_buy_fill(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    order = _get_fill_order(
        snapshot=snapshot,
        event=event,
    )
    execution_notional = (
        calculate_order_notional(
            quantity=event.quantity,
            price=event.price,
        )
    )
    fee = quantize_money(
        event.fee
    )
    reservation_release = (
        calculate_order_notional(
            quantity=event.quantity,
            price=order.limit_price,
        )
        if order is not None
        else ZERO_MONEY
    )
    available_cash = quantize_money(
        snapshot.account.available_cash
        + reservation_release
        - execution_notional
        - fee
    )
    reserved_cash = quantize_money(
        snapshot.account.reserved_cash
        - reservation_release
    )

    _validate_available_cash(
        available_cash
    )

    account = replace(
        snapshot.account,
        available_cash=available_cash,
        reserved_cash=reserved_cash,
        fees_paid=quantize_money(
            snapshot.account.fees_paid
            + fee
        ),
        updated_at=event.event_ts,
    )

    existing_positions = [
        position
        for position in snapshot.positions
        if position.ticker == event.ticker
    ]

    if existing_positions:
        existing_position = existing_positions[0]
        updated_quantity = (
            existing_position.quantity
            + event.quantity
        )
        updated_average_entry_price = (
            calculate_weighted_average_entry_price(
                existing_quantity=(
                    existing_position.quantity
                ),
                existing_average_entry_price=(
                    existing_position.average_entry_price
                ),
                added_quantity=event.quantity,
                added_price=event.price,
            )
        )
        position = replace(
            existing_position,
            quantity=updated_quantity,
            average_entry_price=(
                updated_average_entry_price
            ),
            cost_basis=quantize_money(
                existing_position.cost_basis
                + execution_notional
                + fee
            ),
            current_price=quantize_money(
                event.price
            ),
            fees_paid=quantize_money(
                existing_position.fees_paid
                + fee
            ),
            last_increased_at=event.event_ts,
            last_updated_at=event.event_ts,
        )
    else:
        position = PositionState(
            portfolio_id=snapshot.portfolio_id,
            ticker=event.ticker,
            espn_event_id=event.espn_event_id,
            quantity=event.quantity,
            average_entry_price=quantize_money(
                event.price
            ),
            cost_basis=quantize_money(
                execution_notional
                + fee
            ),
            current_price=quantize_money(
                event.price
            ),
            realized_pnl=ZERO_MONEY,
            fees_paid=fee,
            opened_at=event.event_ts,
            last_increased_at=event.event_ts,
            last_reduced_at=None,
            last_updated_at=event.event_ts,
        )

    updated_snapshot = replace(
        snapshot,
        account=account,
    )
    updated_snapshot = _upsert_position(
        snapshot=updated_snapshot,
        updated_position=position,
    )

    if order is not None:
        updated_snapshot = _upsert_order(
            snapshot=updated_snapshot,
            updated_order=(
                _update_order_after_fill(
                    order=order,
                    filled_quantity=event.quantity,
                    event_ts=event.event_ts,
                )
            ),
        )

    return updated_snapshot


def _apply_sell_fill(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    order = _get_fill_order(
        snapshot=snapshot,
        event=event,
    )
    position = _find_position(
        snapshot=snapshot,
        ticker=event.ticker,
    )

    if event.quantity > position.quantity:
        raise ValueError(
            "Sell fill quantity exceeds position quantity."
        )

    proceeds = calculate_order_notional(
        quantity=event.quantity,
        price=event.price,
    )
    fee = quantize_money(
        event.fee
    )
    allocated_cost_basis = (
        calculate_cost_basis_allocation(
            total_cost_basis=position.cost_basis,
            total_quantity=position.quantity,
            quantity_removed=event.quantity,
        )
    )
    realized_pnl = calculate_realized_pnl(
        proceeds=proceeds,
        allocated_cost_basis=(
            allocated_cost_basis
        ),
        exit_fee=fee,
    )
    remaining_quantity = (
        position.quantity
        - event.quantity
    )

    account = replace(
        snapshot.account,
        available_cash=quantize_money(
            snapshot.account.available_cash
            + proceeds
            - fee
        ),
        realized_pnl=quantize_money(
            snapshot.account.realized_pnl
            + realized_pnl
        ),
        fees_paid=quantize_money(
            snapshot.account.fees_paid
            + fee
        ),
        updated_at=event.event_ts,
    )

    updated_snapshot = replace(
        snapshot,
        account=account,
    )

    if remaining_quantity == 0:
        updated_snapshot = _remove_position(
            snapshot=updated_snapshot,
            ticker=event.ticker,
        )
    else:
        updated_position = replace(
            position,
            quantity=remaining_quantity,
            cost_basis=quantize_money(
                position.cost_basis
                - allocated_cost_basis
            ),
            current_price=quantize_money(
                event.price
            ),
            realized_pnl=quantize_money(
                position.realized_pnl
                + realized_pnl
            ),
            fees_paid=quantize_money(
                position.fees_paid
                + fee
            ),
            last_reduced_at=event.event_ts,
            last_updated_at=event.event_ts,
        )
        updated_snapshot = _upsert_position(
            snapshot=updated_snapshot,
            updated_position=updated_position,
        )

    if order is not None:
        updated_snapshot = _upsert_order(
            snapshot=updated_snapshot,
            updated_order=(
                _update_order_after_fill(
                    order=order,
                    filled_quantity=event.quantity,
                    event_ts=event.event_ts,
                )
            ),
        )

    return updated_snapshot


def _apply_settlement(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    position = _find_position(
        snapshot=snapshot,
        ticker=event.ticker,
    )

    if event.quantity != position.quantity:
        raise ValueError(
            "Settlement quantity must equal the full open "
            "position quantity."
        )

    active_orders = [
        order
        for order in snapshot.orders
        if (
            order.ticker == event.ticker
            and order.is_active
        )
    ]

    if active_orders:
        raise ValueError(
            "All active orders for a ticker must be closed "
            "before settlement."
        )

    proceeds = calculate_order_notional(
        quantity=event.quantity,
        price=event.price,
    )
    fee = quantize_money(
        event.fee
    )
    realized_pnl = calculate_realized_pnl(
        proceeds=proceeds,
        allocated_cost_basis=(
            position.cost_basis
        ),
        exit_fee=fee,
    )
    available_cash = quantize_money(
        snapshot.account.available_cash
        + proceeds
        - fee
    )

    _validate_available_cash(
        available_cash
    )

    account = replace(
        snapshot.account,
        available_cash=available_cash,
        realized_pnl=quantize_money(
            snapshot.account.realized_pnl
            + realized_pnl
        ),
        fees_paid=quantize_money(
            snapshot.account.fees_paid
            + fee
        ),
        updated_at=event.event_ts,
    )

    updated_snapshot = replace(
        snapshot,
        account=account,
    )

    return _remove_position(
        snapshot=updated_snapshot,
        ticker=event.ticker,
    )


def apply_event(
    snapshot: PortfolioSnapshot,
    event: PortfolioEvent,
) -> PortfolioSnapshot:
    validate_event(
        event
    )

    if event.portfolio_id != snapshot.portfolio_id:
        raise ValueError(
            "Event portfolio_id does not match snapshot."
        )

    if event.event_id in snapshot.applied_event_ids:
        raise ValueError(
            "Portfolio event has already been applied. "
            f"Duplicate event_id: {event.event_id}."
        )

    if event.event_sequence <= snapshot.last_event_sequence:
        raise ValueError(
            "Portfolio event_sequence must be greater than "
            "the snapshot last_event_sequence."
        )

    if event.event_ts < snapshot.as_of_ts:
        raise ValueError(
            "Portfolio event timestamp cannot precede the "
            "snapshot as_of timestamp."
        )

    transition_by_event_type = {
        PortfolioEventType.CASH_DEPOSIT: (
            _apply_cash_deposit
        ),
        PortfolioEventType.CASH_WITHDRAWAL: (
            _apply_cash_withdrawal
        ),
        PortfolioEventType.ORDER_PLACED: (
            _apply_order_placed
        ),
        PortfolioEventType.ORDER_CANCELLED: (
            _apply_order_cancelled
        ),
        PortfolioEventType.BUY_FILL: (
            _apply_buy_fill
        ),
        PortfolioEventType.SELL_FILL: (
            _apply_sell_fill
        ),
        PortfolioEventType.SETTLEMENT: (
            _apply_settlement
        ),
    }

    transition = transition_by_event_type.get(
        event.event_type
    )

    if transition is None:
        raise ValueError(
            "Unsupported portfolio event type: "
            f"{event.event_type!r}"
        )

    updated_snapshot = transition(
        snapshot,
        event,
    )
    updated_snapshot = replace(
        updated_snapshot,
        as_of_ts=event.event_ts,
        last_event_sequence=(
            event.event_sequence
        ),
        applied_event_ids=frozenset(
            {
                *updated_snapshot.applied_event_ids,
                event.event_id,
            }
        ),
    )

    if VALIDATE_AFTER_EACH_EVENT:
        validate_snapshot(
            updated_snapshot
        )

    return updated_snapshot


def replay_events(
    snapshot: PortfolioSnapshot,
    events: Iterable[PortfolioEvent],
) -> PortfolioSnapshot:
    ordered_events = sorted(
        list(
            events
        ),
        key=lambda event: event.event_sequence,
    )

    validate_event_sequence(
        events=ordered_events,
        initial_event_sequence=(
            snapshot.last_event_sequence
        ),
        initial_event_ts=snapshot.as_of_ts,
    )

    updated_snapshot = snapshot

    for event in ordered_events:
        updated_snapshot = apply_event(
            snapshot=updated_snapshot,
            event=event,
        )

    return updated_snapshot


def mark_to_market(
    snapshot: PortfolioSnapshot,
    market_prices: Mapping[str, Decimal],
    as_of_ts: datetime,
    require_all_positions: bool = True,
) -> PortfolioSnapshot:
    if as_of_ts < snapshot.as_of_ts:
        raise ValueError(
            "Mark-to-market timestamp cannot precede the "
            "snapshot as_of timestamp."
        )

    missing_tickers = [
        position.ticker
        for position in snapshot.positions
        if position.ticker not in market_prices
    ]

    if (
        require_all_positions
        and missing_tickers
    ):
        raise ValueError(
            "Market prices are missing for active positions. "
            f"Missing tickers: {missing_tickers}"
        )

    updated_positions: list[PositionState] = []

    for position in snapshot.positions:
        if position.ticker not in market_prices:
            updated_positions.append(
                position
            )
            continue

        current_price = quantize_money(
            market_prices[
                position.ticker
            ]
        )

        if (
            current_price < 0
            or current_price > CONTRACT_PAYOUT
        ):
            raise ValueError(
                "Mark-to-market prices must be between zero "
                "and the configured contract payout."
            )

        updated_positions.append(
            replace(
                position,
                current_price=current_price,
                last_updated_at=as_of_ts,
            )
        )

    updated_snapshot = replace(
        snapshot,
        as_of_ts=as_of_ts,
        account=replace(
            snapshot.account,
            updated_at=as_of_ts,
        ),
        positions=tuple(
            sorted(
                updated_positions,
                key=lambda position: position.ticker,
            )
        ),
    )

    validate_snapshot(
        updated_snapshot
    )

    return updated_snapshot
