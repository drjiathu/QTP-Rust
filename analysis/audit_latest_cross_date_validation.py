"""Read-only cross-date receipt, coverage, and provenance audit (stdlib only)."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT = ROOT / 'reports/20260906-latest-cross-date-full'


def audit(out=DEFAULT, require_complete=False):
    manifest = json.loads((out / 'manifest.json').read_text())
    binary = Path(manifest['frozen_binary'])
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == manifest['binary_sha256']
    hashes = json.loads((out / manifest['source_hashes_file']).read_text())
    for name, expected in hashes.items():
        assert hashlib.sha256((out / manifest['source_snapshot'] / name).read_bytes()).hexdigest() == expected
    unchanged = all((ROOT / name).exists() and hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected
                    for name, expected in hashes.items())
    rows = []
    for scope, directory in [('full', out), ('smoke', out / 'smoke')]:
        for day in manifest['days']:
            for market in ('SH', 'SZ'):
                path = directory / f'{day}-{market.lower()}-{scope}-run.json'
                row = dict(date=day, market=market, scope=scope, status='not_started')
                if not path.exists():
                    rows.append(row)
                    continue
                receipt = json.loads(path.read_text())
                assert (receipt['date'], receipt['market'], receipt['scope']) == (day, market, scope)
                assert receipt['command'][0] == str(binary)
                assert ('--symbols' in receipt['command']) == (scope == 'smoke')
                assert ('--include-etfs' in receipt['command']) == (scope == 'full')
                row.update({key: receipt[key] for key in ('status', 'exit_code', 'elapsed_seconds', 'error') if key in receipt})
                if receipt.get('report'):
                    report = json.loads((directory / receipt['report']).read_text())
                    assert not report['diagnostic_window_override']
                    assert report['total_anchors'] == report['matched'] + report['mismatched'] + report['not_comparable']
                    assert report['comparable_anchors'] == report['matched'] + report['mismatched']
                    for key in ('matched', 'mismatched', 'excluded_by_status', 'data_errors', 'missing_source'):
                        assert sum(c[key] for c in report['breakdown'].values()) == report[key]
                        row[key] = report[key]
                    feeds = ('mdl_4_24_0',) if market == 'SH' else ('mdl_6_33_0', 'mdl_6_36_0')
                    expected = sum(s['rows'] for s in manifest['sources'] if s['date'] == day and s['feed'] in feeds)
                    assert report['replay']['input_rows'] == expected
                    if market == 'SZ':
                        assert report['replay']['sz_market_order_policy'] == 'rest_at_last_trade_price'
                    for key in ('breakdown', 'match_tags', 'mismatch_fields', 'not_comparable_reasons', 'omitted_mismatched_records'):
                        row[key] = report[key]
                    row['symbols'] = report['replay']['symbols']
                    row['match_rate'] = report['match_rate']
                    row['not_comparable_rate'] = report['not_comparable_rate']
                    row['pending_groups'] = report['replay']['sz_pending_groups']
                    row['empty_same_side_cancellations'] = report['replay']['sz_empty_same_side_cancellations']
                    row['sample_anomalies'] = [r for r in report['records'] if r['outcome'] not in ('matched', 'excluded_by_status')][:10]
                    row['snapshot_passed'] = receipt['exit_code'] == 0
                rows.append(row)
    full = [r for r in rows if r['scope'] == 'full']
    complete = all(r['status'] in ('completed', 'replay_aborted', 'driver_error') for r in full)
    if require_complete:
        assert complete, 'Six full-market jobs have not all finished'
    heartbeat = datetime.fromisoformat(manifest['heartbeat_at'])
    return dict(as_of=datetime.now(timezone.utc).isoformat(), binary_sha256=manifest['binary_sha256'],
        current_source_same=unchanged, driver_status=manifest['status'],
        heartbeat_age_seconds=(datetime.now(timezone.utc)-heartbeat).total_seconds(),
        all_full_jobs_finished=complete,
        all_full_validations_passed=complete and all(r.get('snapshot_passed', False) for r in full), rows=rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--require-complete', action='store_true')
    args = parser.parse_args()
    print(json.dumps(audit(require_complete=args.require_complete), ensure_ascii=False, indent=2))
