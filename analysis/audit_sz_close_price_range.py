"""Independent all-limit order ledger; diagnostic close-range projection, not a production fix."""
from collections import defaultdict
import json
from pathlib import Path
import re
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'reports/20260907-sz-cross-date-diagnosis'


def units(x):
    v=x*10000
    assert v==int(v)
    return int(v)


def view(orders, low=0, high=10**30):
    levels={49:defaultdict(lambda:[0,0]),50:defaultdict(lambda:[0,0])}
    for r in orders.values():
        if r['remaining']>0 and low<=r['p']<=high:
            v=levels[r['Side']][r['p']];v[0]+=r['remaining'];v[1]+=1
    out={}
    for side,name in [(49,'bid'),(50,'ask')]:
        level=levels[side];q=sum(v[0] for v in level.values());n=sum(p*v[0] for p,v in level.items())
        out[name]=dict(total=q,weighted=((n+q*100//2)//(q*100))*100 if q else None,
            depth=[(p,*level[p]) for p in sorted(level,reverse=side==49)[:10]])
    return out


def run():
    result=[]
    for date,symbols in [('20260320',['300391']),('20260401',['001257','301683']),('20260706',['001248']),('20260806',['001232'])]:
        root=OUT/date
        orders={}
        for r in pq.ParquetFile(root/'mdl_6_33_0.parquet').read().to_pylist():
            if r['SecurityID'] not in symbols:continue
            assert r['OrdType']==50
            key=(r['ChannelNo'],r['ApplSeqNum']);assert key not in orders
            r.update(remaining=r['OrderQty'],p=units(r['Price']));orders[key]=r
        trades=defaultdict(list);cancel_errors=[]
        for r in pq.ParquetFile(root/'mdl_6_36_0.parquet').read().to_pylist():
            if r['SecurityID'] not in symbols:continue
            for side,field in [(49,'BidApplSeqNum'),(50,'OfferApplSeqNum')]:
                if not r[field]:continue
                o=orders[(r['ChannelNo'],r[field])]
                assert o['Side']==side and o['SecurityID']==r['SecurityID']
                if r['ExecType']==52 and o['remaining']!=r['LastQty']:cancel_errors.append(r)
                o['remaining']-=r['LastQty'];assert o['remaining']>=0
            if r['ExecType']==70:trades[r['SecurityID']].append(r)
        assert not cancel_errors
        refs=pq.ParquetFile(root/'mdl_6_28_0.parquet').read().to_pylist()
        for sym in symbols:
            e=next(r for r in refs if r['SecurityID']==sym and r['TradingPhaseCode'].strip()=='E0')
            own={k:v for k,v in orders.items() if v['SecurityID']==sym}
            trade=sorted(trades[sym],key=lambda r:r['ApplSeqNum'])
            before=next(r for r in reversed(trade) if r['TransactTime']<'14:57:00.000')
            p=units(before['LastPx']);lo=(p*9+500)//1000*100;hi=(p*11+500)//1000*100
            full=view(own);ranged=view(own,lo,hi);expected={}
            for side,name,total,weighted in [('Bid','bid','TotalBidQty','WeightedAvgBidPx'),('Ask','ask','TotalOfferQty','WeightedAvgOfferPx')]:
                depth=[(units(e[f'{side}Price{i}']),e[f'{side}Volume{i}'],e[f'NumOrders{side[0] if side=="Bid" else "S"}{i}']) for i in range(1,11) if e[f'{side}Price{i}']]
                expected[name]=dict(total=e[total],weighted=units(e[weighted]),depth=depth)
            def matches(actual):
                return all(actual[s]['depth']==expected[s]['depth'] and actual[s]['total']==expected[s]['total'] and abs(actual[s]['weighted']-expected[s]['weighted'])<=10 for s in ['bid','ask'])
            item=dict(date=date,symbol=sym,orders=len(own),active_orders=sum(v['remaining']>0 for v in own.values()),
                order_types=[50],lifecycle_errors=0,range_base_trade=before,low_units=lo,high_units=hi,
                high_limit_price=str(e['HighLimitPrice']),raw_last_price=str(e['LastPrice']),full=full,ranged=ranged,expected=expected,
                full_matches=matches(full),ranged_matches=matches(ranged))
            failures=json.loads((root/'mismatches.json').read_text())
            rust=next(r for r in failures if r['symbol']==sym and r['anchor']=='market_close')
            for diff in rust['differences']:
                field=diff['field']
                if field in ('bids','asks'):
                    side='bid' if field=='bids' else 'ask'
                    actual=[tuple(map(int,t)) for t in re.findall(r'price_units: (\d+), quantity: (\d+), order_count: (\d+)',diff['actual'])]
                    assert full[side]['depth']==actual
                elif field.startswith('total_'):
                    assert full['bid' if 'bid' in field else 'ask']['total']==int(diff['actual'])
                elif field.startswith('weighted_'):
                    assert full['bid' if 'bid' in field else 'ask']['weighted']==int(re.search(r'\d+',diff['actual']).group())
                else:raise AssertionError(field)
            item['independent_full_view_agrees_with_rust']=True
            stats=dict(trade_count=len(trade),quantity=sum(r['LastQty'] for r in trade),
                       turnover=sum(units(r['LastPx'])*r['LastQty'] for r in trade),
                       last=units(trade[-1]['LastPx']),high=max(units(r['LastPx']) for r in trade),low=min(units(r['LastPx']) for r in trade))
            reference_stats=dict(trade_count=e['TurnNum'],quantity=e['Volume'],turnover=units(e['Turnover']),
                                 last=units(e['LastPrice']),high=units(e['HighPrice']),low=units(e['LowPrice']))
            item['statistics_match']=stats==reference_stats
            result.append(item)
            print(date,sym,'range',lo,hi,'full',matches(full),'filtered',matches(ranged),'totals',[(s,full[s]['total'],ranged[s]['total'],expected[s]['total']) for s in ['bid','ask']],flush=True)
    (OUT/'close-range-audit.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+'\n')


if __name__=='__main__':run()
