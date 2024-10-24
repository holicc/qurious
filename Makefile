check:
	cargo check --all-features

make tpch:
	mkdir -p qurious/tests/tpch/data
	docker run -it -v "$(realpath qurious/tests/tpch/data)":/data ghcr.io/scalytics/tpch-docker:main -vf -s 0.1