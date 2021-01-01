use s3ql::{bucket::*, region, s3_client};

fn client() -> rusoto_s3::S3Client {
    let region = region("us-east-1".to_owned(), "http://localhost:4566".to_owned());
    s3_client(region)
}

#[tokio::test]
async fn create_bucket() {
    let name = "testCreateBucket".to_string();
    let s3 = client();
    let bucket = s3.create_s3_bucket(name.clone(), None).await;

    assert!(bucket.is_ok());

    let has_bucket = s3.has_s3_bucket(name).await;

    assert!(has_bucket.is_ok());
}

#[tokio::test]
async fn delete_bucket() {
    let name = "testToDeleteBucket".to_string();
    let s3 = client();
    let bucket = s3.create_s3_bucket(name.clone(), None).await;

    assert!(bucket.is_ok());

    let deleted_bucket = s3.drop_s3_bucket(name.clone()).await;

    assert!(deleted_bucket.is_ok());

    let has_bucket = s3.has_s3_bucket(name).await;

    assert!(has_bucket.is_err());
}

#[tokio::test]
async fn list_buckets() {
    let name1 = "testCreateBucket1".to_string();
    let name2 = "testCreateBucket2".to_string();
    let s3 = client();
    let _ = s3.create_s3_bucket(name1.clone(), None).await;
    let _ = s3.create_s3_bucket(name2.clone(), None).await;

    let has_bucket1 = s3.has_s3_bucket(name1).await;
    let has_bucket2 = s3.has_s3_bucket(name2).await;

    assert!(has_bucket1.is_ok());
    assert!(has_bucket2.is_ok());

    let list = s3.show_s3_buckets().await;
    assert!(list.is_ok());
}
