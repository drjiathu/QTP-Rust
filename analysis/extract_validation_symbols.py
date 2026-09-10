"""Extract distinct mismatched symbols from a compact or full validation JSON."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

SYMBOL_PATTERN = re.compile(r'"symbol": "([0-9]{6})"')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--prefix", action="append", default=[])
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()

    symbols: set[str] = set()
    current_symbol: str | None = None
    with args.report.open(encoding="utf-8") as report:
        for line in report:
            match = SYMBOL_PATTERN.search(line)
            if match is not None:
                current_symbol = match.group(1)
            elif (
                '"outcome": "mismatched"' in line
                and current_symbol is not None
                and (
                    not args.prefix
                    or any(current_symbol.startswith(prefix) for prefix in args.prefix)
                )
            ):
                symbols.add(current_symbol)

    if args.expected_count is not None and len(symbols) != args.expected_count:
        raise SystemExit(
            f"expected {args.expected_count} symbols, extracted {len(symbols)}"
        )
    print(",".join(sorted(symbols)))


if __name__ == "__main__":
    main()
