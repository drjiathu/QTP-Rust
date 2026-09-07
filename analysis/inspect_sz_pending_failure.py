"""Independent raw-channel context and complete lifecycle for a failed SZ order.

Never infers matching boundaries from the filtered Rust spool. Decimal values are
serialized as strings; quantities and sequence identifiers remain integers.
"""
import argparse
import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq


def inspect(day, channel, sequence, symbol):
    root = Path('/hdd/data/stock/raw_level2_parquet') / f'date={day}'
    context, lifecycle, orders = [], [], []
    sources = []
    for feed in ('mdl_6_33_0', 'mdl_6_36_0'):
        path = root / feed / 'part-0.parquet'
        file = pq.ParquetFile(path)
        sources.append(dict(path=str(path), rows=file.metadata.num_rows,
                            size=path.stat().st_size, schema=str(file.schema_arrow)))
        for batch in file.iter_batches(batch_size=262144):
            in_channel = pc.equal(batch.column('ChannelNo'), channel)
            near = pc.and_(pc.greater_equal(batch.column('ApplSeqNum'), sequence - 8),
                           pc.less_equal(batch.column('ApplSeqNum'), sequence + 25))
            for row in batch.filter(pc.and_(in_channel, near)).to_pylist():
                context.append(dict(feed=feed, **row))
            if feed == 'mdl_6_36_0':
                related = pc.or_(pc.equal(batch.column('BidApplSeqNum'), sequence),
                                 pc.equal(batch.column('OfferApplSeqNum'), sequence))
                selected = pc.and_(in_channel, pc.and_(related, pc.equal(batch.column('SecurityID'), symbol)))
                lifecycle.extend(batch.filter(selected).to_pylist())
            else:
                orders.extend(batch.filter(pc.and_(in_channel, pc.equal(batch.column('ApplSeqNum'), sequence))).to_pylist())
    context.sort(key=lambda r: r['ApplSeqNum'])
    lifecycle.sort(key=lambda r: r['ApplSeqNum'])
    assert len(orders) == 1, orders
    order = orders[0]
    remaining = order['OrderQty']
    traded = cancelled = 0
    for row in lifecycle:
        if row['ExecType'] == 70:
            traded += row['LastQty']
        elif row['ExecType'] == 52:
            cancelled += row['LastQty']
        else:
            raise ValueError(row)
        remaining -= row['LastQty']
        row['remaining_after'] = remaining
        assert remaining >= 0, row
    result = dict(date=day, channel=channel, sequence=sequence, symbol=symbol,
                  sources=sources, order=order, unfiltered_channel_context=context,
                  lifecycle=lifecycle, traded_quantity=traded, cancelled_quantity=cancelled,
                  end_remaining=remaining, execution_prices=sorted(set(str(r['LastPx']) for r in lifecycle if r['ExecType']==70)))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('date')
    parser.add_argument('channel', type=int)
    parser.add_argument('sequence', type=int)
    parser.add_argument('symbol')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = inspect(args.date, args.channel, args.sequence, args.symbol)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str)+'\n')
    print(json.dumps({key: value for key, value in result.items() if key not in ('sources', 'unfiltered_channel_context')}, ensure_ascii=False, default=str))
