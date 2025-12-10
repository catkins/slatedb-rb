#!/bin/bash
set -euo pipefail

echo "--- :debian: Installing dependencies"
apt-get update && apt-get install -y libclang-dev

echo "--- :rust: Installing Rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

echo "--- :bundler: Installing gems"
bundle install

echo "+++ :rust: Compiling native extension"
bundle exec rake compile

echo "+++ :rspec: Running tests"
bundle exec rake spec
