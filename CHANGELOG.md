# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Strongly typed order-book events and SH/SZ Parquet replay and validation.
- FIFO order-book reconstruction with add, trade, and full-remainder cancel.
- Reviewed golden fixtures, Rust core regression tests, and property tests.
- Summaries for five additional validation dates and the post-retirement
  20260828 SH/SZ full-market regression; all comparable snapshots matched.
  Detailed evidence is stored locally, outside version control.

### Changed

- Simplified root documentation and removed the duplicate `COPYING.LESSER` copy;
  `LICENSE` and `COPYING` retain the complete LGPL/GPL terms unchanged.
- Renamed the golden fixture directory to `tests/fixtures/golden` and split the
  two scenarios into independent Rust tests and expected files, preserving all
  event sequences, checkpoint labels and expected values.
- `replay_benchmark` now measures prebuilt core events through `OrderBook::apply`,
  not legacy normalization/replay; its timings are not comparable to old runs.
- Consolidated maintained documentation into usage, implementation, acceptance
  rules and validation evidence, with shared Markdown lint configuration.
- Pruned superseded reports and one-off analysis artifacts, retaining complete
  local evidence, compact diagnostics and a cleanup inventory.
- Removed `reports/` from unpublished commit trees without rewriting published
  remote history; kept local reports and ignored the entire directory.
- Kept reusable analysis Python tools, fixed Ruff diagnostics and formatting,
  and stopped tracking local-only result notebooks without deleting local files.

### Removed

- Breaking API change: retired the QTP legacy module and its public normalization,
  reference-resolution and slice-replay interfaces, including legacy raw records,
  enums and steady timestamps. Use production Parquet APIs or `BookEvent` directly.
- Removed legacy-only adapter tests. Shared core policies and production behavior
  remain unchanged.
- Removed the standalone C++ oracle and its CI build/comparison job. Reviewed
  golden expectations and Rust regression coverage remain; no C++ toolchain is
  needed.
