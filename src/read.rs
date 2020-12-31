use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_s3::{GetObjectError, GetObjectOutput, GetObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;

#[async_trait]
pub trait Readable: S3 {
    async fn read_s3_object(
        &self,
        bucket_name: String,
        key: String,
        if_id_matches: Option<String>,
        if_modified_since: Option<String>,
        if_unmodified_since: Option<String>,
    ) -> Result<GetObjectOutput, RusotoError<GetObjectError>>;

    async fn read_s3_object_body(
        &self,
        bucket_name: String,
        key: String,
        if_id_matches: Option<String>,
        if_modified_since: Option<String>,
        if_unmodified_since: Option<String>,
    ) -> Option<String>;
}

#[async_trait]
impl Readable for S3Client {
    async fn read_s3_object(
        &self,
        bucket_name: String,
        key: String,
        if_id_matches: Option<String>,
        if_modified_since: Option<String>,
        if_unmodified_since: Option<String>,
    ) -> Result<GetObjectOutput, RusotoError<GetObjectError>> {
        let get_object = GetObjectRequest {
            bucket: bucket_name,
            key,
            if_match: if_id_matches,
            if_modified_since,
            if_unmodified_since,
            ..Default::default()
        };

        self.get_object(get_object).await
    }

    async fn read_s3_object_body(
        &self,
        bucket_name: String,
        key: String,
        if_id_matches: Option<String>,
        if_modified_since: Option<String>,
        if_unmodified_since: Option<String>,
    ) -> Option<String> {
        let get_object = GetObjectRequest {
            bucket: bucket_name,
            key,
            if_match: if_id_matches,
            if_modified_since,
            if_unmodified_since,
            ..Default::default()
        };

        match self.get_object(get_object).await {
            Err(_) => None,
            Ok(obj) => {
                if let Some(obj_body) = obj.body {
                    let mut stream = obj_body.into_async_read();
                    let mut body = Vec::new();
                    stream.read_to_end(&mut body).await.ok();

                    String::from_utf8(body).ok()
                } else {
                    None
                }
            }
        }
    }
}
