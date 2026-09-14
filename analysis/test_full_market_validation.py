"""Controller checks; no real replay or raw-data writes."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import run_full_market_validation as runner


def example_report():
    return {
        "diagnostic_window_override": False,
        "total_anchors": 3,
        "comparable_anchors": 2,
        "matched": 2,
        "mismatched": 0,
        "not_comparable": 1,
        "excluded_by_status": 1,
        "data_errors": 0,
        "missing_source": 0,
        "omitted_mismatched_records": 0,
        "breakdown": {
            "stock.market_close": {
                "total": 3,
                "matched": 2,
                "mismatched": 0,
                "not_comparable": 1,
                "excluded_by_status": 1,
                "data_errors": 0,
                "missing_source": 0,
            }
        },
        "replay": {
            "input_rows": 10,
            "scheduled_snapshots": 0,
            "sz_market_order_policy": "rest_at_last_trade_price",
        },
        "continuous_lookback_ms": 0,
        "continuous_lookahead_ms_by_symbol": {
            "000001": 1000,
            "159001": 1100,
            "302132": 3000,
        },
        "records": [{"market": "SZ"}],
    }


class ControllerTests(unittest.TestCase):
    def test_fail_fast_cancels_only_owned_active_groups_and_keeps_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / "live"
            folder.mkdir()
            proc = Mock(pid=12345, returncode=-15)
            proc.poll.return_value = None
            proc.wait.return_value = -15
            manifest = {
                "stop_on_failure": True,
                "status": "running",
                "jobs": [
                    {"id": "bad", "market": "SZ", "status": "mismatched"},
                    {
                        "id": "live",
                        "market": "SH",
                        "status": "running",
                        "pid": 12345,
                        "receipt": "live/receipt.json",
                    },
                    {"id": "pending", "market": "SZ", "status": "pending"},
                ],
            }
            active = {
                "live": (
                    proc,
                    runner.time.monotonic(),
                    folder,
                    {"spool_path": "retained"},
                )
            }
            with patch.object(runner.os, "killpg") as kill:
                self.assertTrue(runner.stop_on_failure(manifest, active, root))
            kill.assert_called_once_with(12345, runner.signal.SIGTERM)
            self.assertEqual(active, {})
            self.assertEqual(manifest["status"], "stopped_on_failure")
            self.assertEqual(manifest["jobs"][1]["status"], "cancelled_after_failure")
            self.assertEqual(manifest["jobs"][2]["status"], "pending")
            receipt = json.loads((folder / "receipt.json").read_text())
            self.assertEqual(receipt["spool_path"], "retained")
            summary = json.loads((root / "summary.json").read_text())
            self.assertFalse(summary["execution_completed_without_failure"])

    def test_fail_fast_disabled_and_no_eligible_references_do_not_stop(self):
        for enabled, status in [
            (False, "mismatched"),
            (True, "passed"),
            (True, "no_eligible_references"),
        ]:
            with patch.object(runner.os, "killpg") as kill:
                self.assertFalse(
                    runner.stop_on_failure(
                        {
                            "stop_on_failure": enabled,
                            "jobs": [{"id": "job", "status": status}],
                        },
                        {},
                        Path("not-created"),
                    )
                )
                kill.assert_not_called()

    def setUp(self):
        self.job = {
            "date": "20260828",
            "market": "SZ",
            "sources": [
                {"feed": "mdl_6_33_0", "rows": 6},
                {"feed": "mdl_6_36_0", "rows": 4},
                {"feed": "mdl_6_28_0", "rows": 20},
            ],
        }

    def test_default_policy_and_exclusions_pass(self):
        self.assertEqual(runner.validate_report(example_report(), self.job), "passed")

    def test_v2_report_and_zero_reference_run(self):
        self.assertEqual(runner.validate_report(example_v2(), self.job), "passed")
        self.assertEqual(
            runner.validate_report(example_v2(False), self.job),
            "no_eligible_references",
        )
        report = example_v2(False)
        report["replay"]["sz_after_close_events"] = 1
        report["run_outcome"] = "failed"
        self.assertEqual(runner.validate_report(report, self.job), "input_error")

    def test_v2_audit_and_detail_conservation(self):
        for mutate in (
            lambda r: r["selection_audit"][0].update(reference_records=3),
            lambda r: r.update(omitted_matched_records=0),
            lambda r: r["coverage"].update(symbols=2),
            lambda r: r.update(run_outcome="no_eligible_references"),
            lambda r: r.update(match_rate=None),
        ):
            report = example_v2()
            mutate(report)
            with self.assertRaises(ValueError):
                runner.validate_report(report, self.job)

    def test_unknown_report_version_is_not_legacy(self):
        for version in (0, 3, "2", None, True):
            report = example_v2()
            report["report_schema_version"] = version
            with self.assertRaises(ValueError):
                runner.validate_report(report, self.job)

    def test_v2_failure_is_not_a_missing_frame_waiver(self):
        for outcome, expected in (
            ("mismatched", "mismatched"),
            ("data_errors", "input_error"),
            ("missing_source", "input_error"),
        ):
            report = example_v2()
            report.update(
                matched=0,
                run_outcome="failed",
                omitted_matched_records=0,
                omitted_failure_records=1,
            )
            report[outcome] = 1
            report["comparable_references"] = int(outcome == "mismatched")
            report["match_rate"] = 0.0 if outcome == "mismatched" else None
            report["breakdown"]["stock.market_close"].update(
                matched=0, comparable=report["comparable_references"]
            )
            report["breakdown"]["stock.market_close"][outcome] = 1
            self.assertEqual(runner.validate_report(report, self.job), expected)

    def test_summary_separates_legacy_and_v2_denominators(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            jobs = []
            for version, report in ((1, example_report()), (2, example_v2())):
                receipt = {
                    "date": str(version),
                    "market": "SZ",
                    "status": "passed",
                    "counts": {
                        k: report.get(k, 0) for k in runner.count_fields(version)
                    },
                    "breakdown": report["breakdown"],
                    "mismatch_fields": {},
                }
                if version == 1:
                    receipt["not_comparable_reasons"] = {}
                else:
                    receipt.update(
                        report_schema_version=2,
                        failure_reasons={},
                        coverage=report["coverage"],
                    )
                runner.save(root / f"{version}.json", receipt)
                jobs.append(
                    {"market": "SZ", "status": "passed", "receipt": f"{version}.json"}
                )
            summary = runner.summarize({"status": "completed", "jobs": jobs}, root)
            versions = summary["markets"]["SZ"]["versions"]
            self.assertEqual(versions["1"]["counts"]["total_anchors"], 3)
            self.assertEqual(versions["2"]["counts"]["selected_references"], 1)
            self.assertNotIn("total_anchors", versions["2"]["counts"])
            self.assertTrue(summary["acceptance_passed"])
            report = example_v2(False)
            receipt = {
                "date": "3",
                "market": "SZ",
                "status": "no_eligible_references",
                "report_schema_version": 2,
                "counts": {k: report[k] for k in runner.V2_COUNT_FIELDS},
                "breakdown": {},
                "mismatch_fields": {},
                "failure_reasons": {},
                "coverage": report["coverage"],
            }
            runner.save(root / "3.json", receipt)
            jobs.append(
                {
                    "market": "SZ",
                    "status": "no_eligible_references",
                    "receipt": "3.json",
                }
            )
            summary = runner.summarize({"status": "completed", "jobs": jobs}, root)
            self.assertFalse(summary["acceptance_passed"])
            self.assertTrue(summary["execution_completed_without_failure"])
            self.assertEqual(summary["failed_jobs"], [])

    def test_mismatch_truncation_is_not_success_or_a_structural_error(self):
        report = example_report()
        for container in (report, report["breakdown"]["stock.market_close"]):
            container.update(matched=1, mismatched=1)
        report["omitted_mismatched_records"] = 1
        self.assertEqual(runner.validate_report(report, self.job), "mismatched")

    def test_missing_source_blocks_acceptance(self):
        report = example_report()
        for container in (report, report["breakdown"]["stock.market_close"]):
            container.update(excluded_by_status=0, missing_source=1)
        self.assertEqual(runner.validate_report(report, self.job), "input_error")

    def test_changed_rules_and_partial_scan_rejected(self):
        report = example_report()
        for field, value in (
            ("diagnostic_window_override", True),
            ("continuous_lookback_ms", 1000),
        ):
            changed = copy.deepcopy(report)
            changed[field] = value
            with self.assertRaises(ValueError):
                runner.validate_report(changed, self.job)
        report["replay"]["input_rows"] = 9
        with self.assertRaises(ValueError):
            runner.validate_report(report, self.job)

    def test_resource_gate_reserves_all_active_peaks(self):
        gib = runner.GIB
        self.assertTrue(
            runner.can_admit(5, 300 * gib, 1000 * gib, 100 * gib, [100 * gib] * 6)
        )
        self.assertFalse(
            runner.can_admit(5, 255 * gib, 1000 * gib, 100 * gib, [100 * gib] * 6)
        )
        self.assertFalse(
            runner.can_admit(5, 300 * gib, 799 * gib, 100 * gib, [100 * gib] * 6)
        )
        self.assertFalse(
            runner.can_admit(0, 300 * gib, 1000 * gib, 19 * gib, [100 * gib])
        )

    def test_job_inventory_is_exhaustive_and_pilot_first(self):
        inputs = [
            {"date": date, "feed": feed, "rows": 10, "status": "ok"}
            for date in ("20260105", "20260828")
            for feeds in runner.FEEDS.values()
            for feed in feeds
        ]
        jobs = runner.make_jobs({"inputs": inputs}, ["20260828"])
        self.assertEqual(len(jobs), 4)
        self.assertTrue(all(j["pilot"] for j in jobs[:2]))
        self.assertEqual(
            {j["id"] for j in jobs},
            {"20260105-SH", "20260105-SZ", "20260828-SH", "20260828-SZ"},
        )

    def test_atomic_save_preserves_integer_and_completed_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            runner.save(path, {"mtime_ns": 1789000000123456789, "status": "running"})
            runner.save(path, {"mtime_ns": 1789000000123456789, "status": "passed"})
            self.assertEqual(
                json.loads(path.read_text())["mtime_ns"], 1789000000123456789
            )
            self.assertFalse(path.with_suffix(".json.tmp").exists())


def example_v2(selected=True):
    count = int(selected)
    report = {
        k: v
        for k, v in example_report().items()
        if k
        in (
            "diagnostic_window_override",
            "replay",
            "continuous_lookback_ms",
            "continuous_lookahead_ms_by_symbol",
        )
    }
    report.update(
        report_schema_version=2,
        run_outcome="passed" if selected else "no_eligible_references",
        selected_references=count,
        comparable_references=count,
        matched=count,
        mismatched=0,
        data_errors=0,
        missing_source=0,
        omitted_matched_records=count,
        omitted_failure_records=0,
        match_rate=1.0 if selected else None,
        records=[],
        breakdown={
            "stock.market_close": {
                "total": 1,
                "comparable": 1,
                "matched": 1,
                "mismatched": 0,
                "data_errors": 0,
                "missing_source": 0,
            }
        }
        if selected
        else {},
        selection_audit=[
            {
                "symbol": "000001",
                "reference_records": 2,
                "status_counts": {"C0": 1, "E0": 1} if selected else {"H0": 2},
                "selected_counts": {"market_close": 1} if selected else {},
                "skipped_counts": {"status_not_applicable": 2 - count},
                "first_samples": {},
                "coverage": "selected" if selected else "no_eligible_references",
            }
        ],
        coverage={
            "symbols": 1,
            "symbols_with_selected_references": count,
            "symbols_without_reference_records": 0,
            "symbols_without_eligible_references": 1 - count,
            "by_stage": {
                "SZ.stock.market_close": {
                    "symbols": 1,
                    "covered_symbols": count,
                    "selected_references": count,
                }
            },
        },
    )
    return report


if __name__ == "__main__":
    unittest.main()
