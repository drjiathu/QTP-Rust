# Changelog

<!-- markdownlint-configure-file {"MD024": {"siblings_only": true}} -->

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-09-17

### Changed

- Reduced repeated replay lookups and reused symbol runtime, best-price and
  cancellation queries; cached validation rules and avoided unnecessary candidate
  work without changing reconstruction or matching semantics.
- Separated large private test modules from production code, and organized
  validation into reference loading, Arrow decoding, Shenzhen closing-price
  handling, candidate comparison and report assembly responsibilities.
- Split Shenzhen pending processing into collection and application steps while
  preserving read-ahead boundaries, error ordering, observer notifications and
  existing partial-failure effects.
- Updated the implementation guide's module navigation. Public Rust APIs, CLI
  options, report schema v2 and Parquet formats remain unchanged from v0.2.0.

### Added

- Regression coverage for equivalent validation entry points, report accounting
  and pending-order processing boundaries. Full-market validation and paired
  performance evidence remain local under ignored `reports/`.

## [0.2.0] - 2026-09-14

### Added

- Independent reference-selection audits, coverage summaries, original source
  row identities, and explicit no-eligible-reference run outcomes.
- A reusable full-market validation controller with frozen inputs and binaries,
  resource-gated concurrency, resume checks, and optional stop-on-failure behavior.
- Regression tests for phase selection, interrupted trading, precision
  compatibility, report accounting, and controller cancellation boundaries.

### Changed

- Breaking report/API change: validation reports now use schema version 2 and
  represent only selected, real reference frames. Outcomes are `Matched`,
  `Mismatched`, `DataError`, and `MissingSource`; virtual anchors and status-based
  exclusions are removed. Rust rejects unsupported report versions; the analysis
  controller reads v1/v2 separately without mixing their denominators.
- Missing phase predecessors and inapplicable reference frames now enter bounded
  selection audits instead of manufacturing comparison failures. Later valid
  transitions remain eligible, and invalid selected fields still fail.
- CLI summaries display coverage and `N/A` for zero comparisons. Validation
  documentation now describes the v2 migration and audit fields.

### Fixed

- Shanghai phase transitions follow native sequence and market-status boundaries
  independently of business-event quote timestamps, including the closing call
  auction boundary.
- References missing both `SeqNo` and `LocalTime` can match the known 12-significant-
  digit turnover rounding pattern. The known Shenzhen upper-limit sentinel variant
  is normalized only when its own source row meets the same missingness gate.
  Other differences still fail; compatibility is audited and never changes replay
  state, original turnover values, or cached candidates.

### Removed

- Superseded fixed-campaign optimization scripts and their old regression-driver
  chain from `analysis/`. The full-date controller, its tests, and the mismatched
  symbol extraction utility remain. Historical evidence stays local in `reports/`.

## [0.1.0] - 2026-09-10

### Added

- Strongly typed order-book events and SH/SZ Parquet replay and validation.
- FIFO order-book reconstruction with add, trade, and full-remainder cancel.
- Reviewed golden fixtures, Rust core regression tests, and property tests.
- A production `qtp-replay` CLI with stock/ETF selection, scheduled snapshots,
  raw snapshot validation, and optional profiling tools.

### Changed

- Simplified root documentation and removed the duplicate `COPYING.LESSER` copy;
  `LICENSE` and `COPYING` retain the complete LGPL/GPL terms unchanged.
- Renamed the golden fixture directory to `tests/fixtures/golden` and split the
  two scenarios into independent Rust tests and expected files, preserving all
  event sequences, checkpoint labels and expected values.
- `replay_benchmark` now measures prebuilt core events through `OrderBook::apply`,
  not legacy normalization/replay; its timings are not comparable to old runs.
- Optimized replay allocations, symbol lookups, reference loading, and validation
  callbacks while preserving regression results.
- Consolidated maintained Chinese documentation into usage, implementation, and
  acceptance rules, with shared Markdown lint configuration. Machine-specific
  validation and performance baselines remain local under ignored `reports/`;
  general profiling instructions are maintained in the usage guide.
- Updated the project description and configured Taplo to use Cargo's official
  manifest schema without weakening Rust or Clippy lint settings.
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
