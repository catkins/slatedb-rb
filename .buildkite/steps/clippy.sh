#!/bin/bash
set -euo pipefail

echo "--- :debian: Installing dependencies"
apt-get update && apt-get install -y libclang-dev

echo "--- :rust: Installing Rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

cd ext/slatedb

echo "+++ :rust: Cargo check"
cargo check --all-targets

echo "+++ :clippy: Clippy"
cargo clippy --all-targets -- -D warnings
