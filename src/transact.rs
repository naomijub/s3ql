use async_trait::async_trait;
use rusoto_core::{ByteStream, RusotoError};

use rusoto_s3::{PutObjectError, PutObjectOutput, PutObjectRequest, S3Client, S3};
use std::collections::HashMap;

pub struct InsertResponse {
    pub object: PutObjectOutput,
    pub id: String,
}

#[async_trait]
pub trait Transact: S3 {
    async fn insert_s3_object(
        &self,
        bucket_name: String,
        metadata: Option<HashMap<String, String>>,
        key: String,
        body: Option<String>,
        object_request: Option<PutObjectRequest>,
    ) -> Result<InsertResponse, RusotoError<PutObjectError>>;

    async fn update_s3_object_body(
        &self,
        bucket_name: String,
        key: String,
        body: String,
        object_request: Option<PutObjectRequest>,
    ) -> Result<InsertResponse, RusotoError<PutObjectError>>;

    async fn update_s3_object_metadata(
        &self,
        bucket_name: String,
        key: String,
        metadata: Option<HashMap<String, String>>,
        object_request: Option<PutObjectRequest>,
    ) -> Result<InsertResponse, RusotoError<PutObjectError>>;
}

#[async_trait]
impl Transact for S3Client {
    async fn insert_s3_object(
        &self,
        bucket_name: String,
        metadata: Option<HashMap<String, String>>,
        key: String,
        body: Option<String>,
        object_request: Option<PutObjectRequest>,
    ) -> Result<InsertResponse, RusotoError<PutObjectError>> {
        if let Some(mut obj) = object_request {
            if !bucket_name.is_empty() {
                obj.bucket = bucket_name;
            }
            if !key.is_empty() {
                obj.key = key;
            }
            if metadata.is_some() {
                obj.metadata = metadata;
            }
            if body.is_some() {
                obj.body = Some(ByteStream::from(body.unwrap().as_bytes().to_vec()));
            }

            match self.put_object(obj).await {
                Err(e) => Err(e),
                Ok(resp) => Ok(InsertResponse {
                    id: resp.e_tag.clone().unwrap_or_default(),
                    object: resp,
                }),
            }
        } else {
            let mut put_object = PutObjectRequest {
                bucket: bucket_name,
                key,
                metadata,
                ..Default::default()
            };
            if body.is_some() {
                put_object.body = Some(ByteStream::from(body.unwrap().as_bytes().to_vec()));
            }

            match self.put_object(put_object).await {
                Err(e) => Err(e),
                Ok(resp) => Ok(InsertResponse {
                    id: resp.e_tag.clone().unwrap_or_default(),
                    object: resp,
                }),
            }
        }
    }

    async fn update_s3_object_body(
        &self,
        bucket_name: String,
        key: String,
        body: String,
        object_request: Option<PutObjectRequest>,
    ) -> Result<InsertResponse, RusotoError<PutObjectError>> {
        if let Some(mut obj) = object_request {
            obj.bucket = bucket_name;
            obj.key = key;
            obj.body = Some(ByteStream::from(body.as_bytes().to_vec()));

            match self.put_object(obj).await {
                Err(e) => Err(e),
                Ok(resp) => Ok(InsertResponse {
                    id: resp.e_tag.clone().unwrap_or_default(),
                    object: resp,
                }),
            }
        } else {
            let put_object = PutObjectRequest {
                bucket: bucket_name,
                key,
                body: Some(ByteStream::from(body.as_bytes().to_vec())),
                ..Default::default()
            };

            match self.put_object(put_object).await {
                Err(e) => Err(e),
                Ok(resp) => Ok(InsertResponse {
                    id: resp.e_tag.clone().unwrap_or_default(),
                    object: resp,
                }),
            }
        }
    }

    async fn update_s3_object_metadata(
        &self,
        bucket_name: String,
        key: String,
        metadata: Option<HashMap<String, String>>,
        object_request: Option<PutObjectRequest>,
    ) -> Result<InsertResponse, RusotoError<PutObjectError>> {
        if let Some(mut obj) = object_request {
            obj.bucket = bucket_name;
            obj.key = key;
            obj.metadata = metadata;

            match self.put_object(obj).await {
                Err(e) => Err(e),
                Ok(resp) => Ok(InsertResponse {
                    id: resp.e_tag.clone().unwrap_or_default(),
                    object: resp,
                }),
            }
        } else {
            let put_object = PutObjectRequest {
                bucket: bucket_name,
                key,
                metadata: metadata,
                ..Default::default()
            };

            match self.put_object(put_object).await {
                Err(e) => Err(e),
                Ok(resp) => Ok(InsertResponse {
                    id: resp.e_tag.clone().unwrap_or_default(),
                    object: resp,
                }),
            }
        }
    }
}
