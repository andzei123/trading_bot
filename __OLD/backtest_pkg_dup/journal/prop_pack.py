"""Step 13: Pack key artifacts into a zip for sharing/backups."""

from __future__ import annotations
import argparse
from pathlib import Path
import zipfile

DEFAULT_PATHS = [
    "backtest/journal/exports_trades/trades_simulated.csv",
    "backtest/journal/exports_reports/summary_total.csv",
    "backtest/journal/exports_reports/summary_by_symbol.csv",
    "backtest/journal/exports_reports/monthly_risk_by_symbol.csv",
    "backtest/journal/exports_reports/best_trades_by_period.csv",
    "backtest/journal/exports_reports/worst_trades_by_period.csv",
]

def main() -> int:
    ap = argparse.ArgumentParser(description="Step 13: pack key artifacts into a zip")
    ap.add_argument("--out", default="prop_snapshot.zip")
    ap.add_argument("--paths", nargs="*", default=DEFAULT_PATHS)
    args = ap.parse_args()

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    added = 0
    with zipfile.ZipFile(outp, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in args.paths:
            pp = Path(p)
            if pp.exists() and pp.is_file():
                z.write(pp, arcname=str(pp).replace("\\", "/"))
                added += 1

    print(f"Wrote: {outp} (files={added})")
    if added == 0:
        print("[WARN] No files were added. Check your export paths.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
