# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Strongly typed raw market-data and normalized order-book events.
- Deterministic legacy normalization and two-stream replay.
- FIFO order-book reconstruction with add, trade, and full-remainder cancel.
- C++ compatibility oracle, reviewed golden fixtures, and property tests.
