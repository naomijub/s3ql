extern crate rusoto_core;
extern crate rusoto_s3;

use rusoto_core::{RusotoError};
use rusoto_s3::{CreateBucketError, CreateBucketOutput, CreateBucketRequest, S3Client, S3};

pub async fn create_bucket(
    s3_client: &S3Client,
    bucket_name: String,
) -> Result<CreateBucketOutput, RusotoError<CreateBucketError>> {
    let create_bucket_req = CreateBucketRequest {
        bucket: bucket_name.clone(),
        ..Default::default()
    };

    s3_client.create_bucket(create_bucket_req).await
}
