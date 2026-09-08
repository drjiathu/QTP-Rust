# Contributing

## Development setup

Install Rust with `rustup`, then clone the repository. The checked-in
`rust-toolchain.toml` selects the supported compiler and components.

## Branches

- `main` contains reviewed, releasable code.
- `develop` is the integration branch for ongoing work.
- Create focused feature branches from `develop` and open pull requests back to
  `develop`. Release pull requests merge `develop` into `main`.

## Required checks

Run these commands before opening a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo deny check
```

`cargo deny check` requires a current standalone `cargo-deny` binary. It may be
installed with a newer Rust toolchain without changing this crate's Rust 1.85.1
minimum supported version.

For Markdown changes, also run (Node.js 22 or newer):

```bash
npx --yes markdownlint-cli2@0.23.2
```

The checked-in `.markdownlint-cli2.jsonc` defines the maintained document scope
and formatting rules, shared with the VS Code markdownlint extension. Code blocks
and tables are exempt from the 80-character prose limit. Frozen validation
reports and source snapshots are excluded; do not reformat evidence artifacts.
`.gitattributes` also exempts frozen source-snapshot `.patch` files from whitespace
checks so unified-diff context lines and recorded hashes remain intact.

The QTP legacy input API and standalone C++ oracle are retired. Rust golden tests
exercise `OrderBook` directly against reviewed, fixed expected output; no C++
toolchain is required. Core behavior changes must explain their effect on the
golden scenarios. Update the fixture only after reviewing each intended state
transition, not by accepting the implementation's output to make a test pass.
See the [golden test notes](tests/fixtures/golden/README.md) for scope and
the test command.

## Licensing

By submitting a contribution, you agree that it is licensed under
`LGPL-3.0-only`, the license of this project.
