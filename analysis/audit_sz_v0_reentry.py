"""Read-only source audit: 300391 V0 resumption, integer order lifecycle ledger.

The visibility model reproduces the current all-limit HideIfCrossing path only;
it is a diagnostic, not a proposed exchange implementation. Compare its first
candidate against the existing Rust report before interpreting hidden orders.
"""
from collections import Counter, defaultdict
from pathlib import Path
import json
import re

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "reports/20260907-sz-cross-date-diagnosis/20260320"
OUTPUT = SOURCE.parent / "v0-reentry-audit.json"
CANDIDATE = 27304188


def units(value):
    scaled = value * 10000
    assert scaled == int(scaled)
    return int(scaled)


def run():
    raw_orders, executions, references = [
        pq.ParquetFile(SOURCE / f"mdl_6_{feed}_0.parquet").read().to_pylist()
        for feed in [33, 36, 28]
    ]
    assert {r["SecurityID"] for r in raw_orders + executions + references} == {"300391"}
    assert {r["ChannelNo"] for r in raw_orders + executions} == {2014}
    assert {r["OrdType"] for r in raw_orders} == {50}
    for stream in [raw_orders, executions]:
        assert all(a["ApplSeqNum"] < b["ApplSeqNum"] for a, b in zip(stream, stream[1:]))
    events = sorted(raw_orders + executions, key=lambda r: r["ApplSeqNum"])
    assert len({r["ApplSeqNum"] for r in events}) == len(events)
    orders = {}
    # Visible aggregated quantities; hidden orders remain in the lifecycle ledger.
    levels = {49: defaultdict(int), 50: defaultdict(int)}
    captured = None
    histories = defaultdict(list)

    def best(side):
        return (max if side == 49 else min)(levels[side], default=None)

    def add_level(order, quantity):
        side, price = order["Side"], order["price_units"]
        levels[side][price] += quantity
        assert levels[side][price] >= 0
        if levels[side][price] == 0:
            del levels[side][price]

    def view(visible_only):
        result = {}
        for side, name in [(49, "bids"), (50, "asks")]:
            aggregates = defaultdict(lambda: [0, 0])
            for o in orders.values():
                if o["Side"] == side and o["remaining"] and (not visible_only or o["visible"]):
                    level = aggregates[o["price_units"]]
                    level[0] += o["remaining"]
                    level[1] += 1
            result[name] = [[p, *aggregates[p]] for p in sorted(aggregates, reverse=side == 49)[:10]]
            result[f"total_{name}"] = sum(v[0] for v in aggregates.values())
        return result

    for r in events:
        seq = r["ApplSeqNum"]
        if "OrdType" in r:
            price, side = units(r["Price"]), r["Side"]
            opposite = best(99 - side)
            hidden = (
                "09:30:00.000" <= r["TransactTime"] < "14:57:00.000"
                and opposite is not None
                and (price >= opposite if side == 49 else price <= opposite)
            )
            assert seq not in orders
            o = dict(r, price_units=price, remaining=r["OrderQty"], visible=not hidden,
                     opposite_best_on_add=opposite, hidden_on_add=hidden)
            orders[seq] = o
            if o["visible"]:
                add_level(o, o["remaining"])
        else:
            involved = []
            for side, field in [(49, "BidApplSeqNum"), (50, "OfferApplSeqNum")]:
                if not r[field]:
                    continue
                o = orders[r[field]]
                assert o["Side"] == side and o["remaining"] >= r["LastQty"]
                if r["ExecType"] == 52:
                    assert o["remaining"] == r["LastQty"]
                else:
                    assert r["ExecType"] == 70
                if o["visible"]:
                    add_level(o, -r["LastQty"])
                o["remaining"] -= r["LastQty"]
                histories[r[field]].append({k: r[k] for k in ["ApplSeqNum", "TransactTime", "ExecType", "LastPx", "LastQty"]})
                involved.append(o)
            if r["ExecType"] == 70:
                # Same bid-first reentry check as OrderBook::apply_trade.
                for o in involved:
                    opposite = best(99 - o["Side"])
                    if not o["visible"] and o["remaining"] and (
                        opposite is None or (o["price_units"] < opposite if o["Side"] == 49 else o["price_units"] > opposite)
                    ):
                        o["visible"] = True
                        add_level(o, o["remaining"])
        if seq == CANDIDATE:
            missing = [dict(o) for o in orders.values() if o["remaining"] and not o["visible"]]
            captured = dict(sequence=seq, quote_time=r["TransactTime"], visible=view(True), ledger=view(False), hidden_orders=missing)
    assert captured is not None
    failures = json.loads((SOURCE / "mismatches.json").read_text())
    first = failures[0]
    assert first["best_candidate_raw_sequence"] == CANDIDATE
    ref = next(r for r in references if r["UpdateTime"] == "10:58:24.000" and r["TradingPhaseCode"].strip() == "T0")
    expected = {}
    for side, name, count_prefix, total in [("Bid", "bids", "B", "TotalBidQty"), ("Ask", "asks", "S", "TotalOfferQty")]:
        expected[name] = [[units(ref[f"{side}Price{i}"]), ref[f"{side}Volume{i}"], ref[f"NumOrders{count_prefix}{i}"]]
                          for i in range(1, 11) if ref[f"{side}Price{i}"]]
        expected[f"total_{name}"] = ref[total]
        diff = next((d for d in first["differences"] if d["field"] == name), None)
        if diff:
            actual = [list(map(int, t)) for t in re.findall(r"price_units: (\d+), quantity: (\d+), order_count: (\d+)", diff["actual"])]
            assert actual == captured["visible"][name]
        else:
            assert captured["visible"][name] == expected[name]
    assert captured["ledger"] == expected
    assert len(captured["hidden_orders"]) == 14
    assert sum(o["remaining"] for o in captured["hidden_orders"]) == 643600
    for o in captured["hidden_orders"]:
        o["executions_before_candidate"] = [r for r in histories[o["ApplSeqNum"]] if r["ApplSeqNum"] <= CANDIDATE]
        o["first_later_execution"] = next((r for r in histories[o["ApplSeqNum"]] if r["ApplSeqNum"] > CANDIDATE), None)
        o["end_of_day_remaining"] = orders[o["ApplSeqNum"]]["remaining"]
        assert o["price_units"] == 4600 and not o["executions_before_candidate"]
    transitions = []
    previous = None
    for r in references:
        phase = r["TradingPhaseCode"].strip()
        if phase != previous:
            transitions.append([r["UpdateTime"], phase])
            previous = phase
    batches = []
    for time in ["10:58:25.000", "11:09:57.000"]:
        oo = [r for r in raw_orders if r["TransactTime"] == time]
        tt = [r for r in executions if r["TransactTime"] == time and r["ExecType"] == 70]
        batches.append(dict(time=time, orders=len(oo), trades=len(tt), trade_prices_units=sorted({units(r["LastPx"]) for r in tt}),
                            first_sequence=min(r["ApplSeqNum"] for r in oo + tt), last_sequence=max(r["ApplSeqNum"] for r in oo + tt)))
    result = dict(date="20260320", symbol="300391", source=str(SOURCE.relative_to(ROOT)),
                  order_count=len(raw_orders), execution_count=len(executions), order_types=dict(Counter(r["OrdType"] for r in raw_orders)),
                  checks=dict(streams_strictly_increasing=True, duplicate_native_sequence=False, lifecycle_errors=0,
                              diagnostic_visible_depth_agrees_with_rust=True, independent_ledger_depth_counts_totals_match_reference=True),
                  transitions=transitions, batches=batches, first_reference_time=ref["UpdateTime"], expected=expected, candidate=captured)
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return result


if __name__ == "__main__":
    run()
