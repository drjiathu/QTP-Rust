"""Independent, projected-column profile for the standard acceptance audit.

No book reconstruction and no reference prices are read here. Run with the
Clara environment (pyarrow required); output is a bounded JSON summary.
"""

from collections import Counter, defaultdict
from pathlib import Path
import json

import pyarrow.parquet as pq

ROOT = Path("/hdd/data/stock/raw_level2_parquet/date=20260828")
PREFIXES = {
    "SH": (("600", "601", "603", "605", "688", "689"),
           ("510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588")),
    "SZ": (("000", "001", "002", "003", "300", "301", "302"), ("159",)),
}


def profile(market):
    feed, status_column = ("MarketData", "InstruStatus") if market == "SH" else ("mdl_6_28_0", "TradingPhaseCode")
    path = ROOT / feed / "part-0.parquet"
    reader = pq.ParquetFile(path)
    counts = {kind: Counter() for kind in ("stock", "etf")}
    previous = {}
    transitions = defaultdict(list)
    bad_order = Counter()
    excluded = defaultdict(set)
    normal_codes = {"START", "OCALL", "TRADE", "CCALL", "CLOSE", "ENDTR"} if market == "SH" else {"S0", "O0", "B0", "T0", "C0", "E0"}
    for batch in reader.iter_batches(batch_size=262144, columns=["SecurityID", "UpdateTime", status_column, "source_row_no"]):
        columns = [batch.column(i).to_pylist() for i in range(4)]
        for symbol, timestamp, status, source_row in zip(*columns):
            if symbol is None or len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit():
                continue
            if symbol.startswith(PREFIXES[market][0]):
                kind = "stock"
            elif symbol.startswith(PREFIXES[market][1]):
                kind = "etf"
            else:
                continue
            status = status.strip()
            counts[kind][status] += 1
            before = previous.get(symbol)
            if before and (timestamp < before[0] or source_row <= before[2]):
                bad_order[symbol] += 1
            if before is None or status != before[1]:
                transitions[symbol].append((timestamp, status))
            previous[symbol] = (timestamp, status, source_row)
            if status not in normal_codes:
                excluded[status].add(symbol)
    return {
        "source": str(path), "source_rows": reader.metadata.num_rows,
        "selected_symbols": len(previous), "status_counts": counts,
        "order_errors": bad_order,
        "excluded_status_symbols": {status: sorted(symbols) for status, symbols in sorted(excluded.items())},
        "excluded_symbol_transitions": {symbol: transitions[symbol] for symbol in sorted(set().union(*excluded.values()))} if excluded else {},
    }


if __name__ == "__main__":
    for market in ("SH", "SZ"):
        print(json.dumps({market: profile(market)}, ensure_ascii=False), flush=True)
