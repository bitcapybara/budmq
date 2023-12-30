fmt:
	cargo fmt -- --check

clippy:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

test:
	cargo test --workspace

pre-push: fmt clippy test

fix:
	cargo fmt
	cargo sort --workspace
	cargo clippy --fix --allow-staged --allow-dirty --all-targets --all-features --workspace -- -D warnings