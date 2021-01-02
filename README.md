# S3QL

Query Language inspired by SQL for S3.

## Usage

TODO

## Local development
For local development `rust`, `cargo`, `python`, `pip` and `docker` are required. To setup a local S3 with `localstsack` you can execute `make setup` and to start a S3 with `localstack` you can execute `make s3`.

* S3 will be at http://localhost:4566

## TODO:
- [ ] more tests
- [ ] example usage
- [ ] docs
- [x] auth (S3CLient can have auth by using directly `rusoto_s3::S3Client` or function `s3_client_with` with feature `auth`)

### Buckets:
- [x] Create Bucket - `create_s3_bucket`
- [x] Drop Bucket - `drop_s3_bucket`
- [x] Has Bucket - `has_s3_bucket`
- [x] Show Buckets - `show_s3_buckets`

### Transactions:
- [x] Insert Object - `insert_s3_object`
- [x] Update Object Metadata - `update_s3_object_metadata`
- [x] Update Object Body - `update_s3_object_body`

### Direct Read:
- [x] Read Object - `read_s3_object`
- [x] Read Object Body - `read_s3_object_body`
- [x] Has Object - `has_s3_object`
- [x] Show Objects in Bucket - `show_s3_objects`

### Query
- [x] Select object content (AWS and localstack-pro ONLY)
- [ ] All of Query Object in bucket
- [ ] Conditionals https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-conditional.html
- [ ] Cast https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-conversion.html
- [ ] Date Functions https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-date.html
- [ ] String Functions https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-string.html