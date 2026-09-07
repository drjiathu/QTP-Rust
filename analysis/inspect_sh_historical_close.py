"""Audit historical SH ETF phases independently of Rust's phase classifier."""
from collections import Counter
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SYMBOLS = ['510300', '513100', '588000']
ROOT = Path('/hdd/data/stock/raw_level2_parquet/date=20260601')
OUT = Path(__file__).resolve().parents[1] / 'reports/20260906-cross-date-pending/20260601-sh-etf-phases.json'


def inspect():
    result = {s: dict(snapshot_status_counts=Counter(), snapshot_transitions=[], tick_statuses=[],
                      trades_after_1457=0, last_trade=None, first_close=None) for s in SYMBOLS}
    for feed, columns in [
        ('MarketData', ['SecurityID', 'UpdateTime', 'InstruStatus', 'source_row_no', 'LastPrice', 'ClosePrice']),
        ('mdl_4_24_0', ['SecurityID', 'TickTime', 'Type', 'TickBSFlag', 'BizIndex', 'Channel', 'Price', 'Qty', 'source_row_no']),
    ]:
        file = pq.ParquetFile(ROOT / feed / 'part-0.parquet')
        for batch in file.iter_batches(batch_size=262144, columns=columns):
            selected = batch.filter(pc.is_in(batch.column('SecurityID'), value_set=pa.array(SYMBOLS)))
            for row in selected.to_pylist():
                item = result[row['SecurityID']]
                if feed == 'MarketData':
                    phase = row['InstruStatus'].strip()
                    item['snapshot_status_counts'][phase] += 1
                    transitions = item['snapshot_transitions']
                    if not transitions or transitions[-1]['InstruStatus'].strip() != phase:
                        transitions.append(row)
                    if phase == 'CLOSE' and item['first_close'] is None:
                        item['first_close'] = row
                elif row['Type'].strip() == 'S':
                    item['tick_statuses'].append(row)
                elif row['Type'].strip() == 'T':
                    item['last_trade'] = row
                    if row['TickTime'] >= '14:57:00':
                        item['trades_after_1457'] += 1
    return result


if __name__ == '__main__':
    result = inspect()
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str)+'\n')
    print(json.dumps(result, ensure_ascii=False, default=str))
