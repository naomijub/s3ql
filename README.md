# S3QL

Query Language inspired by SQL for S3.

## Usage

```
SELECT <matadata> FROM <Bucket> KEY <key>.
```

## Local development
For local development `rust`, `cargo`, `python`, `pip` and `docker` are required. To setup a local S3 with `localstsack` you can execute `make setup` and to start a S3 with `localstack` you can execute `make s3`.

* S3 will be at http://localhost:4566

## TODO:
- [ ] passing tests
- [ ] example usage

### Buckets:
- [x] Create Bucket - `create_s3_bucket`
- [x] Drop Bucket - `drop_s3_bucket`
- [x] Has Bucket - `has_s3_bucket`
- [x] Show Buckets - `show_s3_buckets`

### Transactions:
- [x] Insert Object - `insert_s3_object`
- [ ] Update Object Key - `update_s3_object_key`
- [ ] Update Object Metadata - `update_s3_object_metadata`
- [ ] Update Object Body - `update_s3_object_body`

### Query
- [ ] All of it