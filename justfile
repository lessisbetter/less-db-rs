# List available recipes
default:
    @just --list

# Run all checks (format, lint, test, TypeScript, browser)
check: fmt lint test test-js test-browser

# Format code
fmt:
    cargo fmt --all

# Run clippy linter (less-db-wasm requires wasm32 target due to sqlite-wasm-rs)
lint:
    cargo clippy -p less-db --all-targets -- -D warnings
    cargo clippy -p less-db-wasm --target wasm32-unknown-unknown -- -D warnings

# Run Rust tests (less-db only; less-db-wasm tests run via test-browser)
test *args:
    cargo test -p less-db {{args}}

# Run Rust tests with verbose output
test-v *args:
    cargo test -p less-db {{args}} -- --nocapture

# Run TypeScript type check
typecheck-js:
    cd crates/less-db-wasm/js && npx tsc --noEmit

# Run JS/WASM tests (vitest)
test-js: typecheck-js
    cd crates/less-db-wasm/js && npx vitest run

# Run Rust benchmarks
bench *args:
    cargo bench --workspace {{args}}

# Run browser integration tests (real WASM + real IndexedDB)
test-browser:
    cd crates/less-db-wasm/js && npx vitest run --config vitest.browser.config.ts

# Run browser benchmarks (WASM vs JS vs Dexie)
bench-browser:
    cd crates/less-db-wasm/js && npx vitest bench --config vitest.bench.config.ts

# Build all targets
build:
    cargo build -p less-db
    cargo build -p less-db-wasm --target wasm32-unknown-unknown

# Build release
build-release:
    cargo build -p less-db --release
    cargo build -p less-db-wasm --target wasm32-unknown-unknown --release

# Clean build artifacts
clean:
    cargo clean
