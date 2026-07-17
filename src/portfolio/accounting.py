from decimal import Decimal, ROUND_HALF_UP

from src.portfolio.models import (
    AccountState,
    PositionState,
)
from src.portfolio.portfolio_config import (
    MONEY_QUANTUM,
)


ZERO_MONEY = Decimal("0")


def quantize_money(
    value: Decimal,
) -> Decimal:
    return value.quantize(
        MONEY_QUANTUM,
        rounding=ROUND_HALF_UP,
    )


def calculate_order_notional(
    quantity: int,
    price: Decimal,
) -> Decimal:
    return quantize_money(
        Decimal(quantity)
        * price
    )


def calculate_weighted_average_entry_price(
    existing_quantity: int,
    existing_average_entry_price: Decimal,
    added_quantity: int,
    added_price: Decimal,
) -> Decimal:
    total_quantity = (
        existing_quantity
        + added_quantity
    )

    if total_quantity <= 0:
        raise ValueError(
            "Total quantity must be positive when "
            "calculating average entry price."
        )

    weighted_value = (
        Decimal(existing_quantity)
        * existing_average_entry_price
        + Decimal(added_quantity)
        * added_price
    )

    return quantize_money(
        weighted_value
        / Decimal(total_quantity)
    )


def calculate_position_cost_basis(
    quantity: int,
    average_entry_price: Decimal,
    entry_fees: Decimal = ZERO_MONEY,
) -> Decimal:
    return quantize_money(
        Decimal(quantity)
        * average_entry_price
        + entry_fees
    )


def calculate_cost_basis_allocation(
    total_cost_basis: Decimal,
    total_quantity: int,
    quantity_removed: int,
) -> Decimal:
    if total_quantity <= 0:
        raise ValueError(
            "Total quantity must be positive when "
            "allocating cost basis."
        )

    if quantity_removed <= 0:
        raise ValueError(
            "Quantity removed must be positive when "
            "allocating cost basis."
        )

    if quantity_removed > total_quantity:
        raise ValueError(
            "Quantity removed cannot exceed total quantity."
        )

    if quantity_removed == total_quantity:
        return quantize_money(
            total_cost_basis
        )

    return quantize_money(
        total_cost_basis
        * Decimal(quantity_removed)
        / Decimal(total_quantity)
    )


def calculate_market_value(
    quantity: int,
    current_price: Decimal,
) -> Decimal:
    return quantize_money(
        Decimal(quantity)
        * current_price
    )


def calculate_realized_pnl(
    proceeds: Decimal,
    allocated_cost_basis: Decimal,
    exit_fee: Decimal,
) -> Decimal:
    return quantize_money(
        proceeds
        - allocated_cost_basis
        - exit_fee
    )


def calculate_unrealized_pnl(
    position: PositionState,
) -> Decimal:
    return quantize_money(
        calculate_market_value(
            quantity=position.quantity,
            current_price=position.current_price,
        )
        - position.cost_basis
    )


def calculate_total_market_value(
    positions: tuple[PositionState, ...],
) -> Decimal:
    return quantize_money(
        sum(
            (
                calculate_market_value(
                    quantity=position.quantity,
                    current_price=position.current_price,
                )
                for position in positions
            ),
            ZERO_MONEY,
        )
    )


def calculate_total_cost_basis(
    positions: tuple[PositionState, ...],
) -> Decimal:
    return quantize_money(
        sum(
            (
                position.cost_basis
                for position in positions
            ),
            ZERO_MONEY,
        )
    )


def calculate_total_unrealized_pnl(
    positions: tuple[PositionState, ...],
) -> Decimal:
    return quantize_money(
        sum(
            (
                calculate_unrealized_pnl(
                    position
                )
                for position in positions
            ),
            ZERO_MONEY,
        )
    )


def calculate_total_equity(
    account: AccountState,
    positions: tuple[PositionState, ...],
) -> Decimal:
    return quantize_money(
        account.available_cash
        + account.reserved_cash
        + calculate_total_market_value(
            positions
        )
    )


def calculate_net_contributions(
    account: AccountState,
) -> Decimal:
    return quantize_money(
        account.total_deposits
        - account.total_withdrawals
    )


def calculate_total_pnl(
    account: AccountState,
    positions: tuple[PositionState, ...],
) -> Decimal:
    return quantize_money(
        account.realized_pnl
        + calculate_total_unrealized_pnl(
            positions
        )
    )


def calculate_accounting_balance_difference(
    account: AccountState,
    positions: tuple[PositionState, ...],
) -> Decimal:
    return quantize_money(
        calculate_total_equity(
            account=account,
            positions=positions,
        )
        - (
            calculate_net_contributions(
                account
            )
            + calculate_total_pnl(
                account=account,
                positions=positions,
            )
        )
    )
