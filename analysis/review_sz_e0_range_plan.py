"""Design evidence only: compare pre-auction vs final-trade range projections."""
from collections import Counter
import json
from pathlib import Path
import pyarrow.parquet as pq
from audit_sz_close_price_range import units, view

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "reports/20260907-sz-cross-date-diagnosis"


def run():
    results = []
    for previous in json.loads((BASE / "close-range-audit.json").read_text()):
        date, symbol = previous["date"], previous["symbol"]
        folder = BASE / date
        orders = {}
        for row in pq.ParquetFile(folder / "mdl_6_33_0.parquet").read().to_pylist():
            if row["SecurityID"] != symbol:
                continue
            assert row["OrdType"] == 50
            key = (row["ChannelNo"], row["ApplSeqNum"])
            assert key not in orders
            orders[key] = dict(row, remaining=row["OrderQty"], p=units(row["Price"]))
        trades = []
        for row in pq.ParquetFile(folder / "mdl_6_36_0.parquet").read().to_pylist():
            if row["SecurityID"] != symbol:
                continue
            for side, field in [(49, "BidApplSeqNum"), (50, "OfferApplSeqNum")]:
                if not row[field]:
                    continue
                order = orders[(row["ChannelNo"], row[field])]
                assert order["ApplSeqNum"] < row["ApplSeqNum"] and order["Side"] == side
                if row["ExecType"] == 52:
                    assert order["remaining"] == row["LastQty"]
                else:
                    assert row["ExecType"] == 70
                order["remaining"] -= row["LastQty"]
                assert order["remaining"] >= 0
            if row["ExecType"] == 70:
                trades.append(row)
        trades.sort(key=lambda row: row["ApplSeqNum"])
        pre = next(row for row in reversed(trades) if row["TransactTime"] < "14:57:00.000")
        refs = [r for r in pq.ParquetFile(folder / "mdl_6_28_0.parquet").read().to_pylist() if r["SecurityID"] == symbol]
        e0 = next(r for r in refs if r["TradingPhaseCode"].strip() == "E0")
        expected = {}
        for side, name, total, weighted, prefix in [("Bid", "bid", "TotalBidQty", "WeightedAvgBidPx", "B"), ("Ask", "ask", "TotalOfferQty", "WeightedAvgOfferPx", "S")]:
            expected[name] = dict(total=e0[total], weighted=units(e0[weighted]), depth=[
                (units(e0[f"{side}Price{i}"]), e0[f"{side}Volume{i}"], e0[f"NumOrders{prefix}{i}"])
                for i in range(1, 11) if e0[f"{side}Price{i}"]])
        projections = {}
        for label, trade in [("pre_1457", pre), ("final_trade", trades[-1])]:
            p = units(trade["LastPx"])
            lo, hi = (p * 9 + 500) // 1000 * 100, (p * 11 + 500) // 1000 * 100
            actual = view(orders, lo, hi)
            # Deliberately exact, including weighted prices; no diagnostic epsilon.
            differences = [f"{side}.{field}" for side in ["bid", "ask"]
                           for field in ["total", "weighted", "depth"] if actual[side][field] != expected[side][field]]
            projections[label] = dict(base=str(trade["LastPx"]), time=trade["TransactTime"], sequence=trade["ApplSeqNum"],
                                      lower_units=lo, upper_units=hi, exact_matches=not differences,
                                      differences=differences, actual=actual)
        bounds = Counter((str(r["HighLimitPrice"]), str(r["LowLimitPrice"])) for r in refs)
        result = dict(date=date, symbol=symbol, projections=projections,
                      reference_daily_limits=[dict(high=k[0], low=k[1], frames=v) for k, v in bounds.items()],
                      closing_trade_times=sorted({r["TransactTime"] for r in trades if r["TransactTime"] >= "14:57:00.000"}),
                      expected=expected)
        results.append(result)
        print(date, symbol, [(k, v["base"], v["exact_matches"], v["differences"]) for k, v in projections.items()], flush=True)
    (BASE / "e0-range-plan-review.json").write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    return results


if __name__ == "__main__":
    run()
