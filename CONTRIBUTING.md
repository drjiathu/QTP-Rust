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
cargo test --locked --all-targets --all-features
cargo test --locked --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo deny check
```

`cargo deny check` requires a current standalone `cargo-deny` binary. It may be
installed with a newer Rust toolchain without changing the project's pinned
Rust toolchain.

For Markdown changes, also run (Node.js 22 or newer):

```bash
npx --yes markdownlint-cli2@0.23.2
```

The checked-in `.markdownlint-cli2.jsonc` defines the maintained document scope
and formatting rules, shared with the VS Code markdownlint extension. Code blocks
and tables are exempt from the 80-character prose limit. Frozen validation
reports and source snapshots are excluded; do not reformat evidence artifacts.
`reports/` and `analysis/*.ipynb` are local-only and ignored by Git. Keep reusable
analysis Python tools and their README in version control. Run `ruff check
analysis` and `ruff format --check analysis` when changing those tools; preparing
historical reports and input data is separate from cloning this repository.

Core behavior changes must explain their effect on the fixed golden expectations.
Do not accept implementation output merely to make a test pass; scope and update
rules are in the [golden test notes](tests/fixtures/golden/README.md).

## Licensing

By submitting a contribution, you agree that it is licensed under
`LGPL-3.0-only`, the license of this project.
