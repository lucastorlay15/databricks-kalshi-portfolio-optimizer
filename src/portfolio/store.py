from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import json
from pathlib import Path
from uuid import uuid4

from src.portfolio.accounting import (
    calculate_total_cost_basis,
    calculate_total_equity,
    calculate_total_market_value,
    calculate_total_unrealized_pnl,
)
from src.portfolio.models import (
    OrderSide,
    OrderStatus,
    PortfolioEvent,
    PortfolioEventType,
    PortfolioSnapshot,
)
from src.portfolio.portfolio_config import (
    PORTFOLIO_ACCOUNT_CURRENT_TABLE,
    PORTFOLIO_EVENTS_TABLE,
    PORTFOLIO_ORDERS_CURRENT_TABLE,
    PORTFOLIO_POSITIONS_CURRENT_TABLE,
)
from src.portfolio.transitions import (
    create_empty_snapshot,
    replay_events,
)
from src.portfolio.validation import (
    validate_event_sequence,
    validate_snapshot,
)


EVENT_TABLE_COLUMNS = [
    "event_id",
    "event_sequence",
    "portfolio_id",
    "event_ts",
    "event_type",
    "ticker",
    "espn_event_id",
    "order_id",
    "decision_id",
    "side",
    "quantity",
    "price",
    "fee",
    "cash_amount",
    "event_payload_hash",
]


def _to_json_compatible(
    value,
):
    if is_dataclass(
        value
    ):
        return _to_json_compatible(
            asdict(
                value
            )
        )

    if isinstance(
        value,
        Enum,
    ):
        return value.value

    if isinstance(
        value,
        Decimal,
    ):
        return str(
            value
        )

    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    if isinstance(
        value,
        dict,
    ):
        return {
            str(key): _to_json_compatible(
                item
            )
            for key, item in value.items()
        }

    if isinstance(
        value,
        (list, tuple),
    ):
        return [
            _to_json_compatible(
                item
            )
            for item in value
        ]

    if isinstance(
        value,
        (set, frozenset),
    ):
        return sorted(
            _to_json_compatible(
                item
            )
            for item in value
        )

    return value


def snapshot_to_dict(
    snapshot: PortfolioSnapshot,
) -> dict:
    return _to_json_compatible(
        snapshot
    )


def event_to_dict(
    event: PortfolioEvent,
) -> dict:
    return _to_json_compatible(
        event
    )


def write_json_artifact(
    artifact_path: str,
    payload: dict,
) -> None:
    path = Path(
        artifact_path
    )
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        mode="w",
        encoding="utf-8",
    ) as artifact_file:
        json.dump(
            _to_json_compatible(
                payload
            ),
            artifact_file,
            indent=2,
            sort_keys=True,
        )


def read_json_artifact(
    artifact_path: str,
) -> dict:
    path = Path(
        artifact_path
    )

    if not path.exists():
        raise FileNotFoundError(
            "Portfolio artifact was not found. "
            f"Expected path: {artifact_path}"
        )

    with path.open(
        mode="r",
        encoding="utf-8",
    ) as artifact_file:
        return json.load(
            artifact_file
        )


def _event_payload_hash(
    event: PortfolioEvent,
) -> str:
    canonical_payload = json.dumps(
        event_to_dict(
            event
        ),
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
    )

    return sha256(
        canonical_payload.encode(
            "utf-8"
        )
    ).hexdigest()


def portfolio_event_to_record(
    event: PortfolioEvent,
) -> dict:
    return {
        "event_id": event.event_id,
        "event_sequence": (
            event.event_sequence
        ),
        "portfolio_id": event.portfolio_id,
        "event_ts": event.event_ts,
        "event_type": event.event_type.value,
        "ticker": event.ticker,
        "espn_event_id": event.espn_event_id,
        "order_id": event.order_id,
        "decision_id": event.decision_id,
        "side": (
            event.side.value
            if event.side is not None
            else None
        ),
        "quantity": event.quantity,
        "price": event.price,
        "fee": event.fee,
        "cash_amount": event.cash_amount,
        "event_payload_hash": (
            _event_payload_hash(
                event
            )
        ),
    }


def _normalize_utc_datetime(
    value: datetime,
) -> datetime:
    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )


def record_to_portfolio_event(
    record,
) -> PortfolioEvent:
    side_value = record[
        "side"
    ]

    return PortfolioEvent(
        event_id=record[
            "event_id"
        ],
        event_sequence=int(
            record[
                "event_sequence"
            ]
        ),
        portfolio_id=record[
            "portfolio_id"
        ],
        event_ts=_normalize_utc_datetime(
            record[
                "event_ts"
            ]
        ),
        event_type=PortfolioEventType(
            record[
                "event_type"
            ]
        ),
        ticker=record[
            "ticker"
        ],
        espn_event_id=record[
            "espn_event_id"
        ],
        order_id=record[
            "order_id"
        ],
        decision_id=record[
            "decision_id"
        ],
        side=(
            OrderSide(
                side_value
            )
            if side_value is not None
            else None
        ),
        quantity=int(
            record[
                "quantity"
            ]
        ),
        price=record[
            "price"
        ],
        fee=record[
            "fee"
        ],
        cash_amount=record[
            "cash_amount"
        ],
    )


def _build_event_schema():
    from pyspark.sql.types import (
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    money_type = DecimalType(
        18,
        6,
    )

    return StructType(
        [
            StructField(
                "event_id",
                StringType(),
                False,
            ),
            StructField(
                "event_sequence",
                LongType(),
                False,
            ),
            StructField(
                "portfolio_id",
                StringType(),
                False,
            ),
            StructField(
                "event_ts",
                TimestampType(),
                False,
            ),
            StructField(
                "event_type",
                StringType(),
                False,
            ),
            StructField(
                "ticker",
                StringType(),
                True,
            ),
            StructField(
                "espn_event_id",
                StringType(),
                True,
            ),
            StructField(
                "order_id",
                StringType(),
                True,
            ),
            StructField(
                "decision_id",
                StringType(),
                True,
            ),
            StructField(
                "side",
                StringType(),
                True,
            ),
            StructField(
                "quantity",
                LongType(),
                False,
            ),
            StructField(
                "price",
                money_type,
                True,
            ),
            StructField(
                "fee",
                money_type,
                False,
            ),
            StructField(
                "cash_amount",
                money_type,
                True,
            ),
            StructField(
                "event_payload_hash",
                StringType(),
                False,
            ),
        ]
    )


def _build_account_schema():
    from pyspark.sql.types import (
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    money_type = DecimalType(
        18,
        6,
    )

    return StructType(
        [
            StructField(
                "portfolio_id",
                StringType(),
                False,
            ),
            StructField(
                "schema_version",
                StringType(),
                False,
            ),
            StructField(
                "as_of_ts",
                TimestampType(),
                False,
            ),
            StructField(
                "last_event_sequence",
                LongType(),
                False,
            ),
            StructField(
                "available_cash",
                money_type,
                False,
            ),
            StructField(
                "reserved_cash",
                money_type,
                False,
            ),
            StructField(
                "total_deposits",
                money_type,
                False,
            ),
            StructField(
                "total_withdrawals",
                money_type,
                False,
            ),
            StructField(
                "realized_pnl",
                money_type,
                False,
            ),
            StructField(
                "unrealized_pnl",
                money_type,
                False,
            ),
            StructField(
                "total_market_value",
                money_type,
                False,
            ),
            StructField(
                "total_cost_basis",
                money_type,
                False,
            ),
            StructField(
                "total_equity",
                money_type,
                False,
            ),
            StructField(
                "fees_paid",
                money_type,
                False,
            ),
            StructField(
                "updated_at",
                TimestampType(),
                False,
            ),
        ]
    )


def _build_position_schema():
    from pyspark.sql.types import (
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    money_type = DecimalType(
        18,
        6,
    )

    return StructType(
        [
            StructField(
                "portfolio_id",
                StringType(),
                False,
            ),
            StructField(
                "ticker",
                StringType(),
                False,
            ),
            StructField(
                "espn_event_id",
                StringType(),
                False,
            ),
            StructField(
                "quantity",
                LongType(),
                False,
            ),
            StructField(
                "average_entry_price",
                money_type,
                False,
            ),
            StructField(
                "cost_basis",
                money_type,
                False,
            ),
            StructField(
                "current_price",
                money_type,
                False,
            ),
            StructField(
                "realized_pnl",
                money_type,
                False,
            ),
            StructField(
                "unrealized_pnl",
                money_type,
                False,
            ),
            StructField(
                "market_value",
                money_type,
                False,
            ),
            StructField(
                "fees_paid",
                money_type,
                False,
            ),
            StructField(
                "opened_at",
                TimestampType(),
                False,
            ),
            StructField(
                "last_increased_at",
                TimestampType(),
                False,
            ),
            StructField(
                "last_reduced_at",
                TimestampType(),
                True,
            ),
            StructField(
                "last_updated_at",
                TimestampType(),
                False,
            ),
            StructField(
                "snapshot_as_of_ts",
                TimestampType(),
                False,
            ),
            StructField(
                "last_event_sequence",
                LongType(),
                False,
            ),
        ]
    )


def _build_order_schema():
    from pyspark.sql.types import (
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    money_type = DecimalType(
        18,
        6,
    )

    return StructType(
        [
            StructField(
                "portfolio_id",
                StringType(),
                False,
            ),
            StructField(
                "order_id",
                StringType(),
                False,
            ),
            StructField(
                "ticker",
                StringType(),
                False,
            ),
            StructField(
                "espn_event_id",
                StringType(),
                False,
            ),
            StructField(
                "side",
                StringType(),
                False,
            ),
            StructField(
                "requested_quantity",
                LongType(),
                False,
            ),
            StructField(
                "filled_quantity",
                LongType(),
                False,
            ),
            StructField(
                "remaining_quantity",
                LongType(),
                False,
            ),
            StructField(
                "limit_price",
                money_type,
                False,
            ),
            StructField(
                "reserved_cash",
                money_type,
                False,
            ),
            StructField(
                "status",
                StringType(),
                False,
            ),
            StructField(
                "decision_id",
                StringType(),
                True,
            ),
            StructField(
                "created_at",
                TimestampType(),
                False,
            ),
            StructField(
                "updated_at",
                TimestampType(),
                False,
            ),
            StructField(
                "snapshot_as_of_ts",
                TimestampType(),
                False,
            ),
            StructField(
                "last_event_sequence",
                LongType(),
                False,
            ),
        ]
    )


def ensure_portfolio_tables(
    spark,
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_EVENTS_TABLE} (
            event_id STRING NOT NULL,
            event_sequence BIGINT NOT NULL,
            portfolio_id STRING NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            event_type STRING NOT NULL,
            ticker STRING,
            espn_event_id STRING,
            order_id STRING,
            decision_id STRING,
            side STRING,
            quantity BIGINT NOT NULL,
            price DECIMAL(18, 6),
            fee DECIMAL(18, 6) NOT NULL,
            cash_amount DECIMAL(18, 6),
            event_payload_hash STRING NOT NULL
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_ACCOUNT_CURRENT_TABLE} (
            portfolio_id STRING NOT NULL,
            schema_version STRING NOT NULL,
            as_of_ts TIMESTAMP NOT NULL,
            last_event_sequence BIGINT NOT NULL,
            available_cash DECIMAL(18, 6) NOT NULL,
            reserved_cash DECIMAL(18, 6) NOT NULL,
            total_deposits DECIMAL(18, 6) NOT NULL,
            total_withdrawals DECIMAL(18, 6) NOT NULL,
            realized_pnl DECIMAL(18, 6) NOT NULL,
            unrealized_pnl DECIMAL(18, 6) NOT NULL,
            total_market_value DECIMAL(18, 6) NOT NULL,
            total_cost_basis DECIMAL(18, 6) NOT NULL,
            total_equity DECIMAL(18, 6) NOT NULL,
            fees_paid DECIMAL(18, 6) NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_POSITIONS_CURRENT_TABLE} (
            portfolio_id STRING NOT NULL,
            ticker STRING NOT NULL,
            espn_event_id STRING NOT NULL,
            quantity BIGINT NOT NULL,
            average_entry_price DECIMAL(18, 6) NOT NULL,
            cost_basis DECIMAL(18, 6) NOT NULL,
            current_price DECIMAL(18, 6) NOT NULL,
            realized_pnl DECIMAL(18, 6) NOT NULL,
            unrealized_pnl DECIMAL(18, 6) NOT NULL,
            market_value DECIMAL(18, 6) NOT NULL,
            fees_paid DECIMAL(18, 6) NOT NULL,
            opened_at TIMESTAMP NOT NULL,
            last_increased_at TIMESTAMP NOT NULL,
            last_reduced_at TIMESTAMP,
            last_updated_at TIMESTAMP NOT NULL,
            snapshot_as_of_ts TIMESTAMP NOT NULL,
            last_event_sequence BIGINT NOT NULL
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_ORDERS_CURRENT_TABLE} (
            portfolio_id STRING NOT NULL,
            order_id STRING NOT NULL,
            ticker STRING NOT NULL,
            espn_event_id STRING NOT NULL,
            side STRING NOT NULL,
            requested_quantity BIGINT NOT NULL,
            filled_quantity BIGINT NOT NULL,
            remaining_quantity BIGINT NOT NULL,
            limit_price DECIMAL(18, 6) NOT NULL,
            reserved_cash DECIMAL(18, 6) NOT NULL,
            status STRING NOT NULL,
            decision_id STRING,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            snapshot_as_of_ts TIMESTAMP NOT NULL,
            last_event_sequence BIGINT NOT NULL
        )
        USING DELTA
        """
    )


def _escape_sql_string(
    value: str,
) -> str:
    return value.replace(
        "'",
        "''",
    )


def append_events(
    spark,
    events: list[PortfolioEvent],
) -> None:
    if not events:
        return

    events_by_portfolio: dict[str, list[PortfolioEvent]] = {}

    for event in events:
        events_by_portfolio.setdefault(
            event.portfolio_id,
            [],
        ).append(
            event
        )

    for portfolio_events in events_by_portfolio.values():
        ordered_events = sorted(
            portfolio_events,
            key=lambda event: event.event_sequence,
        )
        validate_event_sequence(
            ordered_events
        )

    ensure_portfolio_tables(
        spark
    )

    records = [
        portfolio_event_to_record(
            event
        )
        for event in events
    ]
    incoming_df = spark.createDataFrame(
        records,
        schema=_build_event_schema(),
    )
    temporary_view_name = (
        "portfolio_events_incoming_"
        f"{uuid4().hex}"
    )
    incoming_df.createOrReplaceTempView(
        temporary_view_name
    )

    conflicting_event_count = spark.sql(
        f"""
        SELECT COUNT(*) AS conflicting_event_count
        FROM {PORTFOLIO_EVENTS_TABLE} AS target
        INNER JOIN {temporary_view_name} AS source
            ON target.portfolio_id = source.portfolio_id
            AND target.event_id = source.event_id
        WHERE target.event_payload_hash
            <> source.event_payload_hash
        """
    ).first()[
        "conflicting_event_count"
    ]

    conflicting_sequence_count = spark.sql(
        f"""
        SELECT COUNT(*) AS conflicting_sequence_count
        FROM {PORTFOLIO_EVENTS_TABLE} AS target
        INNER JOIN {temporary_view_name} AS source
            ON target.portfolio_id = source.portfolio_id
            AND target.event_sequence
                = source.event_sequence
        WHERE target.event_id <> source.event_id
        """
    ).first()[
        "conflicting_sequence_count"
    ]

    if (
        conflicting_event_count > 0
        or conflicting_sequence_count > 0
    ):
        spark.catalog.dropTempView(
            temporary_view_name
        )

        raise ValueError(
            "Incoming portfolio events conflict with "
            "existing event IDs or event sequences."
        )

    spark.sql(
        f"""
        MERGE INTO {PORTFOLIO_EVENTS_TABLE} AS target
        USING {temporary_view_name} AS source
            ON target.portfolio_id = source.portfolio_id
            AND target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT (
            {", ".join(EVENT_TABLE_COLUMNS)}
        ) VALUES (
            {", ".join(f"source.{column}" for column in EVENT_TABLE_COLUMNS)}
        )
        """
    )

    spark.catalog.dropTempView(
        temporary_view_name
    )


def load_events(
    spark,
    portfolio_id: str,
    minimum_event_sequence: int | None = None,
    maximum_event_sequence: int | None = None,
) -> list[PortfolioEvent]:
    escaped_portfolio_id = (
        _escape_sql_string(
            portfolio_id
        )
    )
    sequence_filters: list[str] = []

    if minimum_event_sequence is not None:
        sequence_filters.append(
            "event_sequence >= "
            f"{int(minimum_event_sequence)}"
        )

    if maximum_event_sequence is not None:
        sequence_filters.append(
            "event_sequence <= "
            f"{int(maximum_event_sequence)}"
        )

    additional_filter_sql = (
        ""
        if not sequence_filters
        else " AND "
        + " AND ".join(
            sequence_filters
        )
    )

    event_rows = spark.sql(
        f"""
        SELECT
            {", ".join(EVENT_TABLE_COLUMNS)}
        FROM {PORTFOLIO_EVENTS_TABLE}
        WHERE portfolio_id = '{escaped_portfolio_id}'
            {additional_filter_sql}
        ORDER BY event_sequence
        """
    ).collect()

    return [
        record_to_portfolio_event(
            event_row
        )
        for event_row in event_rows
    ]


def load_snapshot_from_ledger(
    spark,
    portfolio_id: str,
    empty_as_of_ts: datetime | None = None,
) -> PortfolioSnapshot:
    events = load_events(
        spark=spark,
        portfolio_id=portfolio_id,
    )

    if not events:
        if empty_as_of_ts is None:
            raise ValueError(
                "empty_as_of_ts is required when the "
                "portfolio event ledger is empty."
            )

        return create_empty_snapshot(
            portfolio_id=portfolio_id,
            as_of_ts=empty_as_of_ts,
        )

    initial_snapshot = create_empty_snapshot(
        portfolio_id=portfolio_id,
        as_of_ts=events[0].event_ts,
    )

    return replay_events(
        snapshot=initial_snapshot,
        events=events,
    )


def _snapshot_account_record(
    snapshot: PortfolioSnapshot,
) -> dict:
    return {
        "portfolio_id": snapshot.portfolio_id,
        "schema_version": snapshot.schema_version,
        "as_of_ts": snapshot.as_of_ts,
        "last_event_sequence": (
            snapshot.last_event_sequence
        ),
        "available_cash": (
            snapshot.account.available_cash
        ),
        "reserved_cash": (
            snapshot.account.reserved_cash
        ),
        "total_deposits": (
            snapshot.account.total_deposits
        ),
        "total_withdrawals": (
            snapshot.account.total_withdrawals
        ),
        "realized_pnl": (
            snapshot.account.realized_pnl
        ),
        "unrealized_pnl": (
            calculate_total_unrealized_pnl(
                snapshot.positions
            )
        ),
        "total_market_value": (
            calculate_total_market_value(
                snapshot.positions
            )
        ),
        "total_cost_basis": (
            calculate_total_cost_basis(
                snapshot.positions
            )
        ),
        "total_equity": (
            calculate_total_equity(
                account=snapshot.account,
                positions=snapshot.positions,
            )
        ),
        "fees_paid": snapshot.account.fees_paid,
        "updated_at": snapshot.account.updated_at,
    }


def _snapshot_position_records(
    snapshot: PortfolioSnapshot,
) -> list[dict]:
    from src.portfolio.accounting import (
        calculate_market_value,
        calculate_unrealized_pnl,
    )

    return [
        {
            "portfolio_id": position.portfolio_id,
            "ticker": position.ticker,
            "espn_event_id": position.espn_event_id,
            "quantity": position.quantity,
            "average_entry_price": (
                position.average_entry_price
            ),
            "cost_basis": position.cost_basis,
            "current_price": position.current_price,
            "realized_pnl": position.realized_pnl,
            "unrealized_pnl": (
                calculate_unrealized_pnl(
                    position
                )
            ),
            "market_value": calculate_market_value(
                quantity=position.quantity,
                current_price=position.current_price,
            ),
            "fees_paid": position.fees_paid,
            "opened_at": position.opened_at,
            "last_increased_at": (
                position.last_increased_at
            ),
            "last_reduced_at": (
                position.last_reduced_at
            ),
            "last_updated_at": (
                position.last_updated_at
            ),
            "snapshot_as_of_ts": snapshot.as_of_ts,
            "last_event_sequence": (
                snapshot.last_event_sequence
            ),
        }
        for position in snapshot.positions
    ]


def _snapshot_order_records(
    snapshot: PortfolioSnapshot,
) -> list[dict]:
    return [
        {
            "portfolio_id": order.portfolio_id,
            "order_id": order.order_id,
            "ticker": order.ticker,
            "espn_event_id": order.espn_event_id,
            "side": order.side.value,
            "requested_quantity": (
                order.requested_quantity
            ),
            "filled_quantity": (
                order.filled_quantity
            ),
            "remaining_quantity": (
                order.remaining_quantity
            ),
            "limit_price": order.limit_price,
            "reserved_cash": order.reserved_cash,
            "status": order.status.value,
            "decision_id": order.decision_id,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            "snapshot_as_of_ts": snapshot.as_of_ts,
            "last_event_sequence": (
                snapshot.last_event_sequence
            ),
        }
        for order in snapshot.orders
    ]


def _replace_portfolio_slice(
    dataframe,
    table_name: str,
    portfolio_id: str,
) -> None:
    escaped_portfolio_id = (
        _escape_sql_string(
            portfolio_id
        )
    )

    dataframe.write.format(
        "delta"
    ).mode(
        "overwrite"
    ).option(
        "replaceWhere",
        f"portfolio_id = '{escaped_portfolio_id}'",
    ).saveAsTable(
        table_name
    )


def persist_current_projections(
    spark,
    snapshot: PortfolioSnapshot,
) -> None:
    validate_snapshot(
        snapshot
    )
    ensure_portfolio_tables(
        spark
    )

    account_df = spark.createDataFrame(
        [
            _snapshot_account_record(
                snapshot
            )
        ],
        schema=_build_account_schema(),
    )
    positions_df = spark.createDataFrame(
        _snapshot_position_records(
            snapshot
        ),
        schema=_build_position_schema(),
    )
    orders_df = spark.createDataFrame(
        _snapshot_order_records(
            snapshot
        ),
        schema=_build_order_schema(),
    )

    _replace_portfolio_slice(
        dataframe=account_df,
        table_name=(
            PORTFOLIO_ACCOUNT_CURRENT_TABLE
        ),
        portfolio_id=snapshot.portfolio_id,
    )
    _replace_portfolio_slice(
        dataframe=positions_df,
        table_name=(
            PORTFOLIO_POSITIONS_CURRENT_TABLE
        ),
        portfolio_id=snapshot.portfolio_id,
    )
    _replace_portfolio_slice(
        dataframe=orders_df,
        table_name=(
            PORTFOLIO_ORDERS_CURRENT_TABLE
        ),
        portfolio_id=snapshot.portfolio_id,
    )
