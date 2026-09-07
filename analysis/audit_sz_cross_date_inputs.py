"""Extract only affected SZ securities for independent diagnostics; raw files read-only."""
from collections import Counter, defaultdict
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/20260907-sz-cross-date-diagnosis'
RAW = Path('/hdd/data/stock/raw_level2_parquet')


def extract():
    OUT.mkdir(exist_ok=True)
    campaign = json.loads((ROOT/'reports/20260906-random-profiled-full/campaign.json').read_text())
    summary = []
    for job in campaign['rows']:
        if job['market'] != 'SZ':
            continue
        p = ROOT/job['receipt']
        receipt = json.loads(p.read_text())
        if not receipt.get('report'):
            summary.append(dict(date=job['date'], status=receipt['status'], error=receipt.get('error')))
            continue
        report = json.loads((p.parent/receipt['report']).read_text())
        bad = [r for r in report['records'] if r['outcome']=='mismatched']
        assert len(bad) == report['mismatched'] and report['omitted_mismatched_records']==0
        groups = defaultdict(list)
        for r in bad:
            groups[(r['symbol'],r['anchor'])].append(r)
        summary.append(dict(date=job['date'], status='completed', matched=report['matched'], mismatched=len(bad),
            symbols=sorted({r['symbol'] for r in bad}), groups=[dict(symbol=s, anchor=a, count=len(rows),
            first_ms=min(r['reference_time_ms'] for r in rows), last_ms=max(r['reference_time_ms'] for r in rows),
            fields=dict(Counter(f['field'] for r in rows for f in r['differences']))) for (s,a),rows in groups.items()]))
        if not bad:
            continue
        folder=OUT/job['date'];folder.mkdir(exist_ok=True)
        (folder/'mismatches.json').write_text(json.dumps(bad,ensure_ascii=False,indent=2)+'\n')
        symbols=pa.array(sorted({r['symbol'] for r in bad}))
        for feed in ('mdl_6_28_0','mdl_6_33_0','mdl_6_36_0'):
            path=RAW/f"date={job['date']}"/feed/'part-0.parquet'
            f=pq.ParquetFile(path);out=folder/f'{feed}.parquet'
            assert not out.exists(), f'do not overwrite {out}'
            scanned=selected=0
            with pq.ParquetWriter(out,f.schema_arrow) as writer:
                for batch in f.iter_batches(batch_size=262144):
                    scanned+=batch.num_rows
                    subset=batch.filter(pc.is_in(batch.column('SecurityID'),value_set=symbols))
                    selected+=subset.num_rows
                    if subset.num_rows: writer.write_batch(subset)
            assert scanned==f.metadata.num_rows
            print(job['date'],feed,'scanned',scanned,'selected',selected,flush=True)
        (folder/'source.json').write_text(json.dumps(dict(raw_root=str(RAW),symbols=symbols.to_pylist()),indent=2)+'\n')
    (OUT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')


if __name__=='__main__':
    extract()
