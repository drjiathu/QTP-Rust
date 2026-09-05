#!/usr/bin/env python3
"""Reconcile Shenzhen stock E0 mismatches against raw order lifecycles.

The script is intentionally independent of the Rust order-book implementation.
It rebuilds closing quantities from Shenzhen order and execution Parquet files,
places a partially-filled market-order remainder at its last execution price,
and compares the resulting ten-level book with the first normal E0 snapshot.
"""

from __future__ import annotations

import argparse
import csv
import json
import tempfile
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="20260828")
    parser.add_argument("--raw-root", default="/hdd/data/stock/raw_level2_parquet")
    parser.add_argument(
        "--validation-report",
        default="reports/20260828-sz-raw-full-validation.json",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/20260828-sz-e0-stock-reconciliation.csv",
    )
    parser.add_argument(
        "--output-json",
        default="reports/20260828-sz-e0-stock-reconciliation.json",
    )
    return parser.parse_args()


def read_report_records(path: Path) -> list[dict]:
    """Stream top-level validation records without loading the 1.4 GiB report."""
    selected = []
    inside_records = False
    current: list[str] | None = None
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not inside_records:
                if line == '  "records": [\n':
                    inside_records = True
                continue
            if current is None:
                if line == "    {\n":
                    current = [line[4:]]
                elif line.startswith("  ]"):
                    break
                continue
            if line in ("    },\n", "    }\n"):
                current.append("}\n")
                record = json.loads("".join(current))
                if (
                    record["anchor"] == "market_close"
                    and record["outcome"] == "mismatched"
                    and record["symbol"][0] in "03"
                ):
                    selected.append(record)
                current = None
            else:
                current.append(line[4:])
    return selected


def sql_symbols(symbols: list[str]) -> str:
    return ",".join("'" + symbol.replace("'", "''") + "'" for symbol in symbols)


def decimal_units(value: object) -> int | None:
    if value is None:
        return None
    decimal = Decimal(str(value))
    if decimal == 0:
        return None
    scaled = decimal * 10_000
    if scaled != scaled.to_integral_value():
        raise ValueError(f"value is not exactly representable at scale 10_000: {value}")
    return int(scaled)


def rounded_weighted(numerator: int, quantity: int, quantum: int = 100) -> int | None:
    if quantity == 0:
        return None
    divisor = quantity * quantum
    return ((numerator + divisor // 2) // divisor) * quantum


def materialize_inputs(
    connection: duckdb.DuckDBPyConnection, raw_day: Path, symbols: list[str]
) -> None:
    symbol_filter = sql_symbols(symbols)
    order_path = raw_day / "mdl_6_33_0" / "part-0.parquet"
    execution_path = raw_day / "mdl_6_36_0" / "part-0.parquet"
    connection.execute(f"""
        CREATE TABLE target_orders AS
        SELECT ChannelNo AS channel, ApplSeqNum AS order_seq,
               trim(SecurityID) AS symbol,
               CAST(round(Price * 10000) AS BIGINT) AS price_units,
               OrderQty AS order_qty, Side AS side, OrdType AS ord_type,
               TransactTime AS transact_time, LocalTime AS local_time,
               source_row_no
        FROM read_parquet('{order_path.as_posix()}')
        WHERE trim(SecurityID) IN ({symbol_filter})
        """)
    connection.execute(f"""
        CREATE TABLE target_execs AS
        SELECT ChannelNo AS channel, ApplSeqNum AS exec_seq,
               trim(SecurityID) AS symbol,
               BidApplSeqNum AS bid_order_seq,
               OfferApplSeqNum AS ask_order_seq,
               CAST(round(LastPx * 10000) AS BIGINT) AS price_units,
               LastQty AS last_qty, ExecType AS exec_type,
               TransactTime AS transact_time, LocalTime AS local_time,
               source_row_no
        FROM read_parquet('{execution_path.as_posix()}')
        WHERE trim(SecurityID) IN ({symbol_filter})
        """)
    connection.execute("""
        CREATE TABLE target_refs AS
        SELECT channel, exec_seq, symbol, 49 AS side,
               bid_order_seq AS order_seq, last_qty, exec_type,
               transact_time, source_row_no
        FROM target_execs WHERE bid_order_seq > 0
        UNION ALL
        SELECT channel, exec_seq, symbol, 50 AS side,
               ask_order_seq AS order_seq, last_qty, exec_type,
               transact_time, source_row_no
        FROM target_execs WHERE ask_order_seq > 0
        """)
    connection.execute("""
        CREATE TABLE target_ledger AS
        WITH consumed AS (
          SELECT channel, symbol, side, order_seq,
                 sum(last_qty) AS consumed_qty,
                 count(*) AS consume_events,
                 sum(CASE WHEN exec_type = 52 THEN last_qty ELSE 0 END) AS cancel_qty,
                 sum(CASE WHEN exec_type = 70 THEN last_qty ELSE 0 END) AS trade_qty
          FROM target_refs GROUP BY ALL
        )
        SELECT o.*,
               coalesce(c.consumed_qty, 0) AS consumed_qty,
               coalesce(c.cancel_qty, 0) AS cancel_qty,
               coalesce(c.trade_qty, 0) AS trade_qty,
               coalesce(c.consume_events, 0) AS consume_events,
               o.order_qty - coalesce(c.consumed_qty, 0) AS remaining_qty
        FROM target_orders o
        LEFT JOIN consumed c USING (channel, symbol, side, order_seq)
        """)
    connection.execute("""
        CREATE TABLE cancel_mismatches AS
        WITH ordered_refs AS (
          SELECT r.*,
                 coalesce(sum(r.last_qty) OVER (
                   PARTITION BY r.channel,r.symbol,r.side,r.order_seq
                   ORDER BY r.exec_seq
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                 ), 0) AS prior_consumed
          FROM target_refs r
        )
        SELECT r.symbol,r.channel,r.side,r.order_seq,r.exec_seq,
               r.last_qty,o.order_qty-r.prior_consumed AS expected_cancel_qty
        FROM ordered_refs r
        JOIN target_orders o USING(channel,symbol,side,order_seq)
        WHERE r.exec_type=52 AND r.last_qty<>o.order_qty-r.prior_consumed
        """)
    connection.execute("""
        CREATE TABLE active_orders AS
        WITH last_trade AS (
          SELECT r.channel,r.symbol,r.side,r.order_seq,
                 arg_max(x.price_units,r.exec_seq) AS last_trade_price_units,
                 arg_max(x.transact_time,r.exec_seq) AS last_trade_time,
                 max(r.exec_seq) AS last_trade_sequence
          FROM target_refs r
          JOIN target_execs x USING(channel,symbol,exec_seq)
          WHERE r.exec_type=70 GROUP BY ALL
        )
        SELECT l.*,
               t.last_trade_price_units,t.last_trade_time,t.last_trade_sequence,
               CASE WHEN l.ord_type=49
                    THEN t.last_trade_price_units ELSE l.price_units END
                 AS final_price_units
        FROM target_ledger l
        LEFT JOIN last_trade t USING(channel,symbol,side,order_seq)
        WHERE l.remaining_qty>0
        """)
    connection.execute("""
        CREATE TABLE derived_levels AS
        SELECT symbol,side,final_price_units AS price_units,
               sum(remaining_qty)::BIGINT AS quantity,
               count(*)::BIGINT AS order_count
        FROM active_orders
        GROUP BY ALL
        """)


def lifecycle_checks(connection: duckdb.DuckDBPyConnection) -> dict:
    return {
        "duplicate_order_keys": connection.execute("""SELECT count(*) FROM (
                 SELECT channel,symbol,side,order_seq,count(*) n
                 FROM target_orders GROUP BY ALL HAVING n>1)""").fetchone()[0],
        "orphan_execution_references": connection.execute(
            """SELECT count(*) FROM target_refs r ANTI JOIN target_orders o
               USING(channel,symbol,side,order_seq)"""
        ).fetchone()[0],
        "overfilled_orders": connection.execute(
            "SELECT count(*) FROM target_ledger WHERE remaining_qty<0"
        ).fetchone()[0],
        "invalid_full_cancellations": connection.execute(
            "SELECT count(*) FROM cancel_mismatches"
        ).fetchone()[0],
        "active_unpriced_market_orders_without_trade": connection.execute(
            """SELECT count(*) FROM active_orders
               WHERE ord_type=49 AND final_price_units IS NULL"""
        ).fetchone()[0],
    }


def load_e0_rows(
    connection: duckdb.DuckDBPyConnection, raw_day: Path, symbols: list[str]
) -> dict[str, dict]:
    fields = [
        "SecurityID",
        "UpdateTime",
        "source_row_no",
        "TotalBidQty",
        "WeightedAvgBidPx",
        "TotalOfferQty",
        "WeightedAvgOfferPx",
    ]
    for side in ("Ask", "Bid"):
        letter = "S" if side == "Ask" else "B"
        for level in range(1, 11):
            fields.extend(
                [
                    f"{side}Price{level}",
                    f"{side}Volume{level}",
                    f"NumOrders{letter}{level}",
                ]
            )
    field_sql = ",".join(fields)
    snapshot_path = raw_day / "mdl_6_28_0" / "part-0.parquet"
    cursor = connection.execute(f"""
        SELECT {field_sql}
        FROM read_parquet('{snapshot_path.as_posix()}')
        WHERE trim(SecurityID) IN ({sql_symbols(symbols)})
          AND trim(TradingPhaseCode)='E0'
        QUALIFY row_number() OVER(
          PARTITION BY trim(SecurityID) ORDER BY UpdateTime,source_row_no
        )=1
        ORDER BY trim(SecurityID)
        """)
    columns = [description[0] for description in cursor.description]
    return {
        str(row[0]).strip(): dict(zip(columns, row, strict=True))
        for row in cursor.fetchall()
    }


def snapshot_levels(row: dict, side: str) -> list[tuple[int, int, int]]:
    letter = "S" if side == "Ask" else "B"
    levels = []
    for level in range(1, 11):
        price = decimal_units(row[f"{side}Price{level}"])
        quantity = int(row[f"{side}Volume{level}"] or 0)
        order_count = int(row[f"NumOrders{letter}{level}"] or 0)
        if price is not None:
            levels.append((price, quantity, order_count))
    return levels


def derived_view(
    connection: duckdb.DuckDBPyConnection, symbol: str, side: str
) -> tuple[list[tuple[int, int, int]], int, int | None]:
    side_code = 49 if side == "Bid" else 50
    direction = "DESC" if side == "Bid" else "ASC"
    levels = connection.execute(
        f"""SELECT price_units,quantity,order_count FROM derived_levels
            WHERE symbol=? AND side=? ORDER BY price_units {direction} LIMIT 10""",
        [symbol, side_code],
    ).fetchall()
    quantity, numerator = connection.execute(
        """SELECT coalesce(sum(quantity),0),
                  coalesce(sum(price_units*quantity),0)
           FROM derived_levels WHERE symbol=? AND side=?""",
        [symbol, side_code],
    ).fetchone()
    quantity = int(quantity)
    return levels, quantity, rounded_weighted(int(numerator), quantity)


def report_quantity_delta(record: dict) -> int:
    delta = 0
    for difference in record["differences"]:
        if difference["field"] in ("total_bid_quantity", "total_ask_quantity"):
            delta += int(difference["expected"]) - int(difference["actual"])
    return delta


def main() -> None:
    args = parse_args()
    report_path = Path(args.validation_report)
    records = read_report_records(report_path)
    symbols = sorted(record["symbol"] for record in records)
    if len(symbols) != 17:
        raise RuntimeError(
            f"expected 17 mismatched Shenzhen stocks, found {len(symbols)}"
        )

    raw_day = Path(args.raw_root) / f"date={args.date}"
    with tempfile.TemporaryDirectory(prefix="qtp-sz-e0-") as temp_dir:
        database = Path(temp_dir) / "reconciliation.duckdb"
        connection = duckdb.connect(database.as_posix())
        connection.execute("SET threads=8")
        connection.execute("SET memory_limit='12GB'")
        connection.execute(f"SET temp_directory='{temp_dir}/spill'")
        materialize_inputs(connection, raw_day, symbols)
        checks = lifecycle_checks(connection)
        e0_rows = load_e0_rows(connection, raw_day, symbols)

        order_rows = connection.execute(
            """SELECT symbol,side,order_seq,price_units,order_qty,trade_qty,
                      cancel_qty,remaining_qty,transact_time,
                      last_trade_price_units,last_trade_time,last_trade_sequence
               FROM active_orders WHERE ord_type=49
               ORDER BY symbol,side,order_seq"""
        ).fetchall()
        details_by_symbol: dict[str, list[dict]] = defaultdict(list)
        for row in order_rows:
            details_by_symbol[row[0]].append(
                {
                    "side": "bid" if row[1] == 49 else "ask",
                    "order_sequence": row[2],
                    "raw_price_units": row[3],
                    "original_quantity": row[4],
                    "traded_quantity": row[5],
                    "cancelled_quantity": row[6],
                    "closing_remaining_quantity": row[7],
                    "order_time": row[8],
                    "derived_resting_price_units": row[9],
                    "last_trade_time": row[10],
                    "last_trade_sequence": row[11],
                }
            )

        output_rows = []
        record_by_symbol = {record["symbol"]: record for record in records}
        for symbol in symbols:
            reference = e0_rows[symbol]
            full_match = True
            comparison = {}
            for side in ("Bid", "Ask"):
                expected_levels = snapshot_levels(reference, side)
                actual_levels, actual_total, actual_weighted = derived_view(
                    connection, symbol, side
                )
                expected_total = int(
                    reference["TotalBidQty" if side == "Bid" else "TotalOfferQty"]
                )
                expected_weighted = decimal_units(
                    reference[
                        "WeightedAvgBidPx" if side == "Bid" else "WeightedAvgOfferPx"
                    ]
                )
                side_match = (
                    expected_levels == actual_levels
                    and expected_total == actual_total
                    and expected_weighted == actual_weighted
                )
                full_match &= side_match
                comparison[side.lower()] = {
                    "levels_match": expected_levels == actual_levels,
                    "total_quantity_match": expected_total == actual_total,
                    "weighted_price_match": expected_weighted == actual_weighted,
                }

            details = details_by_symbol[symbol]
            lifecycle_closed = all(value == 0 for value in checks.values())
            if not lifecycle_closed:
                classification = "MissingSourceEvent"
            elif full_match:
                classification = "RustBug"
            else:
                classification = "SourceDivergence"
            remaining = sum(item["closing_remaining_quantity"] for item in details)
            output_rows.append(
                {
                    "symbol": symbol,
                    "classification": classification,
                    "lifecycle_closed": lifecycle_closed,
                    "active_market_order_count": len(details),
                    "sides": "+".join(sorted({item["side"] for item in details})),
                    "order_sequences": ";".join(
                        str(item["order_sequence"]) for item in details
                    ),
                    "original_quantity": sum(
                        item["original_quantity"] for item in details
                    ),
                    "traded_quantity": sum(item["traded_quantity"] for item in details),
                    "cancelled_quantity": sum(
                        item["cancelled_quantity"] for item in details
                    ),
                    "closing_remaining_quantity": remaining,
                    "snapshot_minus_rust_quantity": report_quantity_delta(
                        record_by_symbol[symbol]
                    ),
                    "derived_resting_prices": ";".join(
                        f"{item['order_sequence']}:{item['derived_resting_price_units']}"
                        for item in details
                    ),
                    "independent_book_matches_e0": full_match,
                    "bid_checks": comparison["bid"],
                    "ask_checks": comparison["ask"],
                    "root_cause": (
                        "partially filled OrdType=49 market-order remainder must rest "
                        "at its last execution price; Rust currently keeps it hidden"
                    ),
                }
            )
        connection.close()

    summary = {
        "trading_day": int(args.date),
        "symbols": len(symbols),
        "classifications": {
            name: sum(row["classification"] == name for row in output_rows)
            for name in ("RustBug", "SourceDivergence", "MissingSourceEvent")
        },
        "active_market_orders": sum(
            row["active_market_order_count"] for row in output_rows
        ),
        "original_quantity": sum(row["original_quantity"] for row in output_rows),
        "traded_quantity": sum(row["traded_quantity"] for row in output_rows),
        "cancelled_quantity": sum(row["cancelled_quantity"] for row in output_rows),
        "closing_remaining_quantity": sum(
            row["closing_remaining_quantity"] for row in output_rows
        ),
        "snapshot_minus_rust_quantity": sum(
            row["snapshot_minus_rust_quantity"] for row in output_rows
        ),
        "all_independent_books_match_e0": all(
            row["independent_book_matches_e0"] for row in output_rows
        ),
        "lifecycle_checks": checks,
    }
    artifact = {
        "method": {
            "input_orders": str(raw_day / "mdl_6_33_0" / "part-0.parquet"),
            "input_executions": str(raw_day / "mdl_6_36_0" / "part-0.parquet"),
            "input_snapshots": str(raw_day / "mdl_6_28_0" / "part-0.parquet"),
            "rust_validation_report": str(report_path),
            "market_order_remainder_price": "last ExecType=70 execution price",
            "stock_weighted_price_quantum_units": 100,
        },
        "summary": summary,
        "rows": output_rows,
        "active_market_order_details": dict(details_by_symbol),
    }

    csv_path = Path(args.output_csv)
    json_path = Path(args.output_json)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_fields = [
        "symbol",
        "classification",
        "lifecycle_closed",
        "active_market_order_count",
        "sides",
        "order_sequences",
        "original_quantity",
        "traded_quantity",
        "cancelled_quantity",
        "closing_remaining_quantity",
        "snapshot_minus_rust_quantity",
        "derived_resting_prices",
        "independent_book_matches_e0",
        "root_cause",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_fields)
        writer.writeheader()
        for row in output_rows:
            writer.writerow({field: row[field] for field in csv_fields})
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(artifact, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
