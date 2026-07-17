from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum


class PortfolioEventType(str, Enum):
    CASH_DEPOSIT = "cash_deposit"
    CASH_WITHDRAWAL = "cash_withdrawal"
    ORDER_PLACED = "order_placed"
    ORDER_CANCELLED = "order_cancelled"
    BUY_FILL = "buy_fill"
    SELL_FILL = "sell_fill"
    SETTLEMENT = "settlement"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class PortfolioEvent:
    event_id: str
    event_sequence: int
    portfolio_id: str
    event_ts: datetime
    event_type: PortfolioEventType
    ticker: str | None = None
    espn_event_id: str | None = None
    order_id: str | None = None
    decision_id: str | None = None
    side: OrderSide | None = None
    quantity: int = 0
    price: Decimal | None = None
    fee: Decimal = Decimal("0")
    cash_amount: Decimal | None = None


@dataclass(frozen=True)
class OrderState:
    order_id: str
    portfolio_id: str
    ticker: str
    espn_event_id: str
    side: OrderSide
    requested_quantity: int
    filled_quantity: int
    limit_price: Decimal
    reserved_cash: Decimal
    status: OrderStatus
    created_at: datetime
    updated_at: datetime
    decision_id: str | None = None

    @property
    def remaining_quantity(self) -> int:
        return (
            self.requested_quantity
            - self.filled_quantity
        )

    @property
    def is_active(self) -> bool:
        return self.status in {
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        }


@dataclass(frozen=True)
class PositionState:
    portfolio_id: str
    ticker: str
    espn_event_id: str
    quantity: int
    average_entry_price: Decimal
    cost_basis: Decimal
    current_price: Decimal
    realized_pnl: Decimal
    fees_paid: Decimal
    opened_at: datetime
    last_increased_at: datetime
    last_reduced_at: datetime | None
    last_updated_at: datetime


@dataclass(frozen=True)
class AccountState:
    portfolio_id: str
    available_cash: Decimal
    reserved_cash: Decimal
    total_deposits: Decimal
    total_withdrawals: Decimal
    realized_pnl: Decimal
    fees_paid: Decimal
    updated_at: datetime


@dataclass(frozen=True)
class PortfolioSnapshot:
    portfolio_id: str
    schema_version: str
    as_of_ts: datetime
    last_event_sequence: int
    account: AccountState
    positions: tuple[PositionState, ...] = field(
        default_factory=tuple
    )
    orders: tuple[OrderState, ...] = field(
        default_factory=tuple
    )
    applied_event_ids: frozenset[str] = field(
        default_factory=frozenset
    )
