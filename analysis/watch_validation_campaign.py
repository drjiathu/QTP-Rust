"""Refresh combined results until both independently scheduled batches end."""
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import json
import time
import traceback

from run_latest_cross_date_validation import save
from summarize_validation_campaign import OUT, ROOT, write_summary


def main():
    with (OUT/'campaign-monitor.log').open('x',buffering=1) as log, redirect_stdout(log), redirect_stderr(log):
        try:
            while True:
                result = write_summary()
                status = dict(heartbeat_at=datetime.now(timezone.utc).isoformat(), status='running',
                    completed=result['completed_market_jobs'], expected=result['expected_market_jobs'])
                if result['all_jobs_finished']:
                    status.update(status='finished', all_snapshot_validations_passed=result['all_snapshot_validations_passed'])
                    save(OUT/'campaign-monitor.json',status)
                    return int(not result['all_snapshot_validations_passed'])
                for directory in (OUT, ROOT/'reports/20260906-latest-cross-date-full'):
                    manifest = json.loads((directory/'manifest.json').read_text())
                    if manifest['status']=='running':
                        age=(datetime.now(timezone.utc)-datetime.fromisoformat(manifest['heartbeat_at'])).total_seconds()
                        if age>600:
                            status.update(status='stale_driver', directory=str(directory), heartbeat_age_seconds=age)
                    elif manifest['status']=='driver_error':
                        status.update(status='driver_error',directory=str(directory))
                save(OUT/'campaign-monitor.json',status)
                print(status,flush=True)
                if status['status']!='running':
                    return 1
                time.sleep(30)
        except Exception:
            print(traceback.format_exc(),flush=True)
            save(OUT/'campaign-monitor.json',dict(status='monitor_error',error=traceback.format_exc()))
            return 1


if __name__=='__main__':
    raise SystemExit(main())
