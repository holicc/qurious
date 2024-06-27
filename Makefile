check:
	cargo check --all-features
test-pg:
	docker compose up -d && cargo test --features postgres && docker compose down