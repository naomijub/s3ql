setup:
	pip install localstack

s3:
	localstack start

test:
	cargo test