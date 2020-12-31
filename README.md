# S3QL

Query Language inspired by SQL for S3.

## Usage

```
SELECT <matadata> FROM <Bucket> KEY <key>.
```

## Local development
For local development `rust`, `cargo`, `python`, `pip` and `docker` are required. To setup a local S3 with `localstsack` you can execute `make setup` and to start a S3 with `localstack` you can execute `make s3`.

* S3 will be at http://localhost:4566