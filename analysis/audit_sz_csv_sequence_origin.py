"""Stream a bounded CSV prefix from cold archive to verify a Parquet inversion's origin."""
import csv
from decimal import Decimal
import io
import json
from pathlib import Path
import subprocess
import time

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
ARCHIVE=Path('/mnt/nas/data/L2_raw_data/L2_datayes/20260616/20260616_mdl_6_33_0.csv.7z')
PARQUET=Path('/hdd/data/stock/raw_level2_parquet/date=20260616/mdl_6_33_0/part-0.parquet')
FIRST,LAST=66683320,66683430


def audit():
    start=time.monotonic()
    process=subprocess.Popen(['7z','x','-so',str(ARCHIVE),'20260616_mdl_6_33_0.csv'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    count=0;carry=b'';samples=[]
    assert process.stdout is not None
    header=process.stdout.readline().decode('utf-8-sig').rstrip('\r\n')
    last_progress=0
    try:
        while count<LAST:
            chunk=process.stdout.read(8*1024*1024)
            if not chunk:raise RuntimeError(f'Unexpected EOF at data row {count}')
            data=carry+chunk;n=data.count(b'\n')
            if count+n>=FIRST:
                lines=data.split(b'\n')
                for i,line in enumerate(lines[:-1],count+1):
                    if FIRST<=i<=LAST:samples.append((i,line.decode('utf-8').rstrip('\r')))
                carry=lines[-1]
            else:carry=data[data.rfind(b'\n')+1:]
            count+=n
            if count-last_progress>=10_000_000:
                print('read_csv_rows',count,'elapsed',round(time.monotonic()-start,1),flush=True);last_progress=count
    finally:
        if process.poll() is None:process.terminate()
        process.stdout.close()
        process.wait()
    fields=next(csv.reader([header]))
    rows=[dict(zip(fields,next(csv.reader([line]))),source_row_no=number) for number,line in samples]
    f=pq.ParquetFile(PARQUET);idx=f.schema_arrow.get_field_index('source_row_no');reference=[]
    for i in range(f.num_row_groups):
        stat=f.metadata.row_group(i).column(idx).statistics
        if stat and stat.min<=LAST and stat.max>=FIRST:
            t=f.read_row_group(i)
            reference.extend(t.filter(pc.and_(pc.greater_equal(t['source_row_no'],FIRST),pc.less_equal(t['source_row_no'],LAST))).to_pylist())
    ref={r['source_row_no']:r for r in reference}
    assert len(rows)==LAST-FIRST+1==len(ref)
    for row in rows:
        p=ref[row['source_row_no']]
        for k in fields:
            if isinstance(p[k],(int,Decimal)):assert Decimal(row[k])==p[k],(row['source_row_no'],k)
            else:assert row[k]==p[k],(row['source_row_no'],k)
    before=next(r for r in rows if r['source_row_no']==66683411)
    after=next(r for r in rows if r['source_row_no']==66683412)
    assert int(before['ApplSeqNum'])==21524148 and int(after['ApplSeqNum'])==21524032
    result=dict(archive=str(ARCHIVE),parquet=str(PARQUET),data_row_range=[FIRST,LAST],
                checked_rows=len(rows),all_original_fields_equal=True,inversion_present_in_csv=True,
                crc_note='Archive header matches Parquet provenance; stopped after prefix, no full member CRC verification.',
                elapsed_seconds=time.monotonic()-start,rows=rows)
    out=ROOT/'reports/20260907-sz-cross-date-diagnosis/csv-origin-audit.json'
    out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('CSV inversion confirmed; 111 rows, all original fields match Parquet.',flush=True)
    return result


if __name__=='__main__':audit()
