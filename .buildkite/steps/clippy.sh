#!/bin/bash
set -euo pipefail

# Install libclang for bindgen
apt-get update && apt-get install -y libclang-dev

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

# Run cargo check and clippy
cd ext/slatedb
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
