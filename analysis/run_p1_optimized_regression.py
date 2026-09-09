"""Compare the three P1 optimizations against the frozen P0 fifteen-day campaign."""

import json

import run_p0_optimized_regression as campaign

BASELINE = campaign.ROOT / "reports/20260909-p0-optimized-full-regression"
campaign.OUT = campaign.ROOT / "reports/20260909-p1-optimized-full-regression"
campaign.OLD_PRIMARY = BASELINE
campaign.OLD_ADDITIONAL = BASELINE
campaign.DAYS = json.loads((BASELINE / "manifest.json").read_text())["baseline_days"]
campaign.CAMPAIGN_LABEL = "P1（基线 P0）"
campaign.STAGE = "p1_optimized"
campaign.shared.OUT = campaign.OUT


if __name__ == "__main__":
    raise SystemExit(campaign.main())
