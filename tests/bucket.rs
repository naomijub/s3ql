use s3ql::{bucket::*, region, s3_client};

fn client() -> rusoto_s3::S3Client {
    let region = region("us-east-1".to_owned(), "http://localhost:4566".to_owned());
    s3_client(region)
}

#[tokio::test]
async fn create_bucket() {
    let name = "test_create_bucket".to_string();
    let s3 = client();
    let bucket = s3.create_s3_bucket(name.clone(), None).await;

    assert!(bucket.is_ok());

    let has_bucket = s3.has_s3_bucket(name).await;

    assert!(has_bucket.is_ok());
}

#[tokio::test]
async fn delete_bucket() {
    let name = "test_to_delete_bucket".to_string();
    let s3 = client();
    let bucket = s3.create_s3_bucket(name.clone(), None).await;

    assert!(bucket.is_ok());

    let deleted_bucket = s3.drop_s3_bucket(name.clone()).await;

    assert!(deleted_bucket.is_ok());

    let has_bucket = s3.has_s3_bucket(name).await;

    assert!(has_bucket.is_err());
}
