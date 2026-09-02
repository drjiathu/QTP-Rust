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

Changes to legacy compatibility behaviour must update the C++ oracle or explain
why no golden change is required. Never replace the golden fixture without
reviewing the state transition that changed.

## Licensing

By submitting a contribution, you agree that it is licensed under
`LGPL-3.0-only`, the license of this project.
