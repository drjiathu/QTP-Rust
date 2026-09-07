"""Check native source-row order in the failing SZ channel, without sorting it away."""
import json
from pathlib import Path
import pyarrow.compute as pc
import pyarrow.parquet as pq

root=Path('/hdd/data/stock/raw_level2_parquet/date=20260616')
out=Path(__file__).resolve().parents[1]/'reports/20260907-sz-cross-date-diagnosis/sequence-20260616.json'
results=[]
for feed in ('mdl_6_33_0','mdl_6_36_0'):
    f=pq.ParquetFile(root/feed/'part-0.parquet')
    prev=None;count=0;regressions=[];context=[]
    columns=['ChannelNo','ApplSeqNum','SecurityID','TransactTime','LocalTime','source_row_no','SeqNo']
    for batch in f.iter_batches(batch_size=262144,columns=columns):
        s=batch.filter(pc.equal(batch.column('ChannelNo'),2015))
        if not s.num_rows:continue
        count+=s.num_rows
        seq=s.column('ApplSeqNum')
        indexes=pc.indices_nonzero(pc.less_equal(seq.slice(1),seq.slice(0,len(seq)-1))).to_pylist()
        if prev and seq[0].as_py()<=prev['ApplSeqNum']:
            regressions.append(dict(previous=prev,current=s.slice(0,1).to_pylist()[0]))
        for i in indexes:
            pair=s.slice(i,2).to_pylist();regressions.append(dict(previous=pair[0],current=pair[1]))
        near=pc.and_(pc.greater_equal(seq,21524000),pc.less_equal(seq,21524200))
        context.extend(s.filter(near).to_pylist())
        prev=s.slice(s.num_rows-1,1).to_pylist()[0]
    item=dict(feed=feed,channel_rows=count,regressions=regressions,context=context)
    results.append(item)
    print(feed,'rows',count,'regressions',len(regressions),regressions[:3],flush=True)
out.write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n')
