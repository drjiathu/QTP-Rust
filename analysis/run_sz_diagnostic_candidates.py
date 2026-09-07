"""Explicitly labelled filtered/phase-remapped diagnostic fixtures, never raw-source acceptance."""
from pathlib import Path
import json
import hashlib
import subprocess
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'reports/20260907-sz-cross-date-diagnosis'
RAW=Path('/hdd/data/stock/raw_level2_parquet')
BIN=ROOT/'target/validation-binaries/20260906-latest-cross-date-full/qtp-replay'


def fixture(day,symbols,label,remap=False,align_first_resumption=False):
    root=OUT/'fixtures'/label
    sources=[]
    for feed in ['mdl_6_28_0','mdl_6_33_0','mdl_6_36_0']:
        p=OUT/day/f'{feed}.parquet'
        t=pq.ParquetFile(p).read();t=t.filter(pc.is_in(t['SecurityID'],value_set=pa.array(symbols)))
        if remap and feed=='mdl_6_28_0':
            codes=pa.array(['H0      ' if v.strip()=='V0' else v for v in t['TradingPhaseCode'].to_pylist()],type=pa.large_string())
            t=t.set_column(t.schema.get_field_index('TradingPhaseCode'),'TradingPhaseCode',codes)
        if align_first_resumption and feed=='mdl_6_28_0':
            # Causal probe only: align phase detection with the independently
            # observed first auction order burst; verify matched candidates
            # against original reference timestamps separately.
            times=pa.array(['10:58:25.000' if v=='10:58:24.000' else v for v in t['UpdateTime'].to_pylist()],type=pa.large_string())
            t=t.set_column(t.schema.get_field_index('UpdateTime'),'UpdateTime',times)
        original=RAW/f'date={day}'/feed/'part-0.parquet'
        meta={k:v for k,v in pq.ParquetFile(original).metadata.metadata.items() if k!=b'ARROW:schema'}
        meta.update({b'qtp.diagnostic.derivative':b'true',b'qtp.diagnostic.symbols':','.join(symbols).encode(),
                     b'qtp.diagnostic.phase_remap':b'V0_to_H0' if remap else b'none',b'qtp.diagnostic.original_path':str(original).encode(),
                     b'qtp.diagnostic.phase_time_shift':b'10:58:24_to_10:58:25' if align_first_resumption else b'none'})
        dest=root/f'date={day}'/feed/'part-0.parquet';dest.parent.mkdir(parents=True,exist_ok=True)
        assert not dest.exists()
        pq.write_table(t.replace_schema_metadata(meta),dest)
        sources.append(dict(path=str(dest),rows=t.num_rows,original=str(original),phase_remap=remap,align_first_resumption=align_first_resumption))
    (root/'diagnostic-provenance.json').write_text(json.dumps(sources,indent=2)+'\n')
    return root


def run(label,day,symbols,root,lookahead=None):
    report=OUT/f'{label}.json'
    cmd=[str(BIN),'validate','--date',day,'--market','SZ','--symbols',','.join(symbols),'--raw-root',str(root),
         '--temp-root',str(ROOT/'target/sz-diagnostic-spool'),'--report',str(report),'--retain-matched-records']
    if lookahead:cmd+=['--continuous-lookahead',lookahead]
    with (OUT/f'{label}.log').open('x') as f:
        proc=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
    receipt=dict(command=cmd,exit_code=proc.returncode,binary_sha256=hashlib.sha256(BIN.read_bytes()).hexdigest(),
                 diagnostic_only=True,report=str(report))
    (OUT/f'{label}-run.json').write_text(json.dumps(receipt,indent=2)+'\n')
    if report.exists():
        r=json.loads(report.read_text())
        print(label,{k:r[k] for k in ['matched','mismatched','data_errors','missing_source']},flush=True)
    else:print(label,'ERROR',(OUT/f'{label}.log').read_text()[-1000:],flush=True)


if __name__=='__main__':
    symbols=['159977','159980','159992'];root=fixture('20260706',symbols,'etf-original')
    run('etf-1s','20260706',symbols,root)
    run('etf-3s','20260706',symbols,root,'3s')
    symbols=['300391'];root=fixture('20260320',symbols,'stock-original')
    run('stock-original','20260320',symbols,root)
    root=fixture('20260320',symbols,'stock-v0-remapped',True)
    run('stock-v0-remapped','20260320',symbols,root)
