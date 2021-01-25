use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_s3::{
    CreateBucketError, CreateBucketOutput, CreateBucketRequest, DeleteBucketError,
    DeleteBucketRequest, HeadBucketError, HeadBucketRequest, ListBucketsError, ListBucketsOutput,
    S3Client, S3,
};

#[async_trait]
pub trait Bucket: S3 {
    async fn create_s3_bucket(
        &self,
        bucket_name: String,
        bucket_req: Option<CreateBucketRequest>,
    ) -> Result<CreateBucketOutput, RusotoError<CreateBucketError>>;

    async fn drop_s3_bucket(
        &self,
        bucket_name: String,
    ) -> Result<(), RusotoError<DeleteBucketError>>;

    async fn has_s3_bucket(&self, bucket_name: String) -> Result<(), RusotoError<HeadBucketError>>;

    async fn show_s3_buckets(&self) -> Result<ListBucketsOutput, RusotoError<ListBucketsError>>;
}

#[async_trait]
impl Bucket for S3Client {
    async fn create_s3_bucket(
        &self,
        bucket_name: String,
        bucket_req: Option<CreateBucketRequest>,
    ) -> Result<CreateBucketOutput, RusotoError<CreateBucketError>> {
        if let Some(mut req) = bucket_req {
            if !bucket_name.is_empty() {
                req.bucket = bucket_name;
            }

            self.create_bucket(req).await
        } else {
            let create_bucket_req = CreateBucketRequest {
                bucket: bucket_name,
                ..CreateBucketRequest::default()
            };

            self.create_bucket(create_bucket_req).await
        }
    }

    async fn drop_s3_bucket(
        &self,
        bucket_name: String,
    ) -> Result<(), RusotoError<DeleteBucketError>> {
        let delete_bucket_req = DeleteBucketRequest {
            bucket: bucket_name,
        };

        self.delete_bucket(delete_bucket_req).await
    }

    async fn has_s3_bucket(&self, bucket_name: String) -> Result<(), RusotoError<HeadBucketError>> {
        let delete_bucket_req = HeadBucketRequest {
            bucket: bucket_name,
        };

        self.head_bucket(delete_bucket_req).await
    }

    async fn show_s3_buckets(&self) -> Result<ListBucketsOutput, RusotoError<ListBucketsError>> {
        self.list_buckets().await
    }
}
