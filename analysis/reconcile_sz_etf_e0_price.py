#!/usr/bin/env python3
"""Explain Shenzhen ETF E0 last-price mismatches from raw trades."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="20260828")
    parser.add_argument("--raw-root", default="/hdd/data/stock/raw_level2_parquet")
    parser.add_argument(
        "--validation-report",
        default="reports/20260828-sz-etf-after-market-remainder-fix.json",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/20260828-sz-etf-e0-close-price-reconciliation.csv",
    )
    parser.add_argument(
        "--output-json",
        default="reports/20260828-sz-etf-e0-close-price-reconciliation.json",
    )
    return parser.parse_args()


def round_weighted(numerator: int, quantity: int, quantum: int) -> int:
    divisor = quantity * quantum
    return ((numerator + divisor // 2) // divisor) * quantum


def main() -> None:
    args = parse_args()
    report_path = Path(args.validation_report)
    with report_path.open(encoding="utf-8") as handle:
        report = json.load(handle)
    records = [
        record
        for record in report["records"]
        if record["anchor"] == "market_close"
        and record["outcome"] == "mismatched"
        and record["symbol"].startswith("159")
    ]
    symbols = sorted(record["symbol"] for record in records)
    if len(symbols) != 29:
        raise RuntimeError(f"expected 29 mismatched ETFs, found {len(symbols)}")

    symbol_filter = ",".join(f"'{symbol}'" for symbol in symbols)
    raw_day = Path(args.raw_root) / f"date={args.date}"
    executions = raw_day / "mdl_6_36_0" / "part-0.parquet"
    snapshots = raw_day / "mdl_6_28_0" / "part-0.parquet"
    connection = duckdb.connect()
    connection.execute("SET threads=8")
    rows = connection.execute(f"""
        WITH trades AS (
          SELECT trim(SecurityID) AS symbol,ApplSeqNum AS sequence,
                 CAST(LastPx*10000 AS BIGINT) AS price_units,LastQty AS quantity,
                 TransactTime AS trade_time,
                 CAST(substr(TransactTime,1,2) AS BIGINT)*3600000+
                 CAST(substr(TransactTime,4,2) AS BIGINT)*60000+
                 CAST(substr(TransactTime,7,2) AS BIGINT)*1000+
                 CAST(substr(TransactTime,10,3) AS BIGINT) AS time_ms
          FROM read_parquet('{executions.as_posix()}')
          WHERE trim(SecurityID) IN ({symbol_filter}) AND ExecType=70
        ), last_trade AS (
          SELECT symbol,arg_max(sequence,sequence) AS last_sequence,
                 arg_max(time_ms,sequence) AS last_ms,
                 arg_max(trade_time,sequence) AS last_trade_time,
                 arg_max(price_units,sequence) AS last_trade_price_units
          FROM trades GROUP BY symbol
        ), one_minute AS (
          SELECT t.symbol,sum(t.price_units*t.quantity)::HUGEINT AS numerator,
                 sum(t.quantity)::BIGINT AS quantity,count(*)::BIGINT AS trade_count
          FROM trades t JOIN last_trade l USING(symbol)
          WHERE t.time_ms BETWEEN l.last_ms-60000 AND l.last_ms
          GROUP BY t.symbol
        ), closing_trades AS (
          SELECT symbol,count(*)::BIGINT AS trade_count
          FROM trades WHERE time_ms>=53820000 GROUP BY symbol
        ), e0 AS (
          SELECT trim(SecurityID) AS symbol,UpdateTime AS e0_time,
                 CAST(LastPrice*10000 AS BIGINT) AS e0_price_units
          FROM read_parquet('{snapshots.as_posix()}')
          WHERE trim(SecurityID) IN ({symbol_filter})
            AND trim(TradingPhaseCode)='E0'
          QUALIFY row_number() OVER(
            PARTITION BY trim(SecurityID) ORDER BY UpdateTime,source_row_no
          )=1
        ), phases AS (
          SELECT trim(SecurityID) AS symbol,
                 arg_max(
                   CASE WHEN trim(TradingPhaseCode)='T0'
                        THEN CAST(LastPrice*10000 AS BIGINT) END,
                   CASE WHEN trim(TradingPhaseCode)='T0' THEN source_row_no END
                 ) AS last_t0_price_units,
                 arg_max(
                   CASE WHEN trim(TradingPhaseCode)='C0'
                        THEN CAST(LastPrice*10000 AS BIGINT) END,
                   CASE WHEN trim(TradingPhaseCode)='C0' THEN source_row_no END
                 ) AS last_c0_price_units
          FROM read_parquet('{snapshots.as_posix()}')
          WHERE trim(SecurityID) IN ({symbol_filter}) GROUP BY symbol
        )
        SELECT e.symbol,e.e0_time,e.e0_price_units,
               l.last_trade_time,l.last_trade_price_units,l.last_sequence,
               p.last_t0_price_units,p.last_c0_price_units,
               o.numerator,o.quantity,o.trade_count,
               coalesce(c.trade_count,0) AS closing_auction_trade_count
        FROM e0 e JOIN last_trade l USING(symbol)
        JOIN one_minute o USING(symbol)
        LEFT JOIN closing_trades c USING(symbol)
        LEFT JOIN phases p USING(symbol)
        ORDER BY e.symbol
        """).fetchall()
    connection.close()

    output_rows = []
    for row in rows:
        calculated = round_weighted(int(row[8]), int(row[9]), 10)
        output_rows.append(
            {
                "symbol": row[0],
                "e0_time": row[1],
                "e0_price_units": row[2],
                "last_trade_time": row[3],
                "last_trade_price_units": row[4],
                "last_trade_sequence": row[5],
                "last_t0_price_units": row[6],
                "last_c0_price_units": row[7],
                "one_minute_trade_quantity": row[9],
                "one_minute_trade_count": row[10],
                "closing_auction_trade_count": row[11],
                "calculated_close_price_units": calculated,
                "formula_matches_e0": calculated == row[2],
                "reason": (
                    "no closing-auction trade; E0 uses the volume-weighted average "
                    "of trades in the 60 seconds through the final trade, rounded "
                    "to the ETF CNY 0.001 price quantum"
                ),
            }
        )

    summary = {
        "trading_day": int(args.date),
        "symbols": len(output_rows),
        "without_closing_auction_trade": sum(
            row["closing_auction_trade_count"] == 0 for row in output_rows
        ),
        "formula_matches_e0": sum(row["formula_matches_e0"] for row in output_rows),
        "price_quantum_units": 10,
        "price_quantum_cny": "0.001",
    }
    artifact = {
        "sources": {
            "executions": str(executions),
            "snapshots": str(snapshots),
            "validation_report": str(report_path),
        },
        "summary": summary,
        "rows": output_rows,
    }
    csv_path = Path(args.output_csv)
    json_path = Path(args.output_json)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_rows[0].keys())
        writer.writeheader()
        writer.writerows(output_rows)
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(artifact, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
