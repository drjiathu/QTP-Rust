"""Read-only audit of full-market validation receipts and aggregate consistency."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT = ROOT/'reports/20260906-rest-at-last-trade-full'


def audit(out=DEFAULT, require_complete=False):
    manifest = json.loads((out/'manifest.json').read_text())
    assert manifest['days'] == ['20260828', '20260601', '20260806']
    binary = Path(manifest['frozen_binary'])
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == manifest['binary_sha256']
    rows = []
    for day in manifest['days']:
        for market in ('SH', 'SZ'):
            receipt = out/f'{day}-{market.lower()}-full-run.json'
            if not receipt.exists():
                rows.append(dict(date=day, market=market, status='not_started'))
                continue
            run = json.loads(receipt.read_text())
            assert run['date'] == day and run['market'] == market and run['scope'] == 'full'
            assert '--include-etfs' in run['command'] and '--symbols' not in run['command']
            assert run['command'][0] == str(binary)
            item = {k: run[k] for k in ('date','market','status')}
            for key in ('exit_code', 'elapsed_seconds', 'error'):
                if key in run:
                    item[key] = run[key]
            if run['report']:
                report = json.loads((out/run['report']).read_text())
                assert sum(v['total'] for v in report['breakdown'].values()) == report['total_anchors']
                assert report['total_anchors'] == report['matched'] + report['mismatched'] + report['not_comparable']
                assert sum(v['matched'] for v in report['breakdown'].values()) == report['matched']
                for value in report['breakdown'].values():
                    assert value['comparable'] == value['matched'] + value['mismatched']
                    assert value['total'] == value['comparable'] + value['not_comparable']
                    assert value['not_comparable'] == value['excluded_by_status'] + value['data_errors'] + value['missing_source']
                for key in ('mismatched', 'excluded_by_status', 'data_errors', 'missing_source'):
                    assert sum(v[key] for v in report['breakdown'].values()) == report[key]
                assert not report['diagnostic_window_override']
                if market == 'SZ':
                    assert report['replay']['sz_market_order_policy'] == 'rest_at_last_trade_price'
                input_feeds = ('mdl_4_24_0',) if market == 'SH' else ('mdl_6_33_0', 'mdl_6_36_0')
                expected_rows = sum(s['rows'] for s in manifest['sources']
                                    if s['date'] == day and s['feed'] in input_feeds)
                assert report['replay']['input_rows'] == expected_rows
                for key, value in run['counts'].items():
                    assert report[key] == value
                for key in ('matched','mismatched','excluded_by_status','data_errors','missing_source','breakdown','mismatch_fields','not_comparable_reasons'):
                    item[key] = report[key]
                item['snapshot_passed'] = run['exit_code'] == 0
                comparable = report['matched'] + report['mismatched']
                item['comparable_match_rate'] = report['matched'] / comparable if comparable else None
                item['not_comparable_rate'] = report['not_comparable'] / report['total_anchors'] if report['total_anchors'] else None
                item['replayed_symbols'] = report['replay']['symbols']
                item['applied_events'] = report['replay']['applied_events']
                item['pending_groups'] = report['replay']['sz_pending_groups']
                item['empty_same_side_cancellations'] = report['replay']['sz_empty_same_side_cancellations']
            rows.append(item)
    complete = all(r['status'] in ('completed', 'replay_aborted') for r in rows)
    if require_complete:
        assert complete, 'Six requested jobs have not all finished'
    return dict(binary_sha256=manifest['binary_sha256'], all_jobs_finished=complete,
                all_snapshot_validations_passed=complete and all(r.get('snapshot_passed',False) for r in rows),
                rows=rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--require-complete', action='store_true')
    args = parser.parse_args()
    print(json.dumps(audit(require_complete=args.require_complete), ensure_ascii=False, indent=2))
