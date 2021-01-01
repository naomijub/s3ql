use s3ql::{bucket::*, read::*, region, s3_client, transact::*};
use std::collections::HashMap;

fn client() -> rusoto_s3::S3Client {
    let region = region("us-east-1".to_owned(), "http://localhost:4566".to_owned());
    s3_client(region)
}

pub const BUCKET: &'static str = "transactObjectsBucket";
pub const UPDATE_BUCKET: &'static str = "updateTransactObjectsBucket";

#[tokio::test]
async fn insert_object() {
    let s3 = client();
    let bucket = s3.create_s3_bucket(BUCKET.to_string(), None).await;

    assert!(bucket.is_ok());
    let mut map = HashMap::new();
    map.insert("tx-time".to_string(), "2007-19-01T11:12:00-000".to_string());

    let body = "{\"hello\": \"world\"}";

    let insert = s3.insert_s3_object(
        BUCKET.to_string(),
        Some(map),
        "key".to_string(),
        Some(body.to_string()),
        None,
    );

    assert!(insert.await.is_ok());

    assert!(s3
        .has_s3_object(BUCKET.to_string(), "key".to_string(), None, None, None)
        .await
        .is_ok());
}

#[tokio::test]
async fn update_object_body() {
    let s3 = client();
    let bucket = s3.create_s3_bucket(BUCKET.to_string(), None).await;

    assert!(bucket.is_ok());

    let body = "{\"hello\": \"world\"}";

    let insert = s3.insert_s3_object(
        BUCKET.to_string(),
        None,
        "key".to_string(),
        Some(body.to_string()),
        None,
    );

    assert!(insert.await.is_ok());

    assert!(s3
        .has_s3_object(BUCKET.to_string(), "key".to_string(), None, None, None)
        .await
        .is_ok());

    let read_obj = s3
        .read_s3_object_body(BUCKET.to_string(), "key".to_string(), None, None, None)
        .await;
    assert!(read_obj.is_some());
    assert_eq!(read_obj.unwrap(), "{\"hello\": \"world\"}");

    let update = s3.update_s3_object_body(
        BUCKET.to_string(),
        "key".to_string(),
        "this is a new body".to_string(),
        None,
    );
    assert!(update.await.is_ok());

    let read_obj = s3
        .read_s3_object_body(BUCKET.to_string(), "key".to_string(), None, None, None)
        .await;
    assert!(read_obj.is_some());
    assert_eq!(read_obj.unwrap(), "this is a new body");
}

#[tokio::test]
async fn update_object_meta() {
    let s3 = client();
    let bucket = s3.create_s3_bucket(UPDATE_BUCKET.to_string(), None).await;

    assert!(bucket.is_ok());

    let insert = s3.insert_s3_object(
        UPDATE_BUCKET.to_string(),
        None,
        "meta-key".to_string(),
        None,
        None,
    );

    assert!(insert.await.is_ok());

    assert!(s3
        .has_s3_object(
            UPDATE_BUCKET.to_string(),
            "meta-key".to_string(),
            None,
            None,
            None
        )
        .await
        .is_ok());

    let read_obj = s3
        .read_s3_object(
            UPDATE_BUCKET.to_string(),
            "meta-key".to_string(),
            None,
            None,
            None,
        )
        .await;
    assert!(read_obj.is_ok());
    assert_eq!(read_obj.unwrap().metadata, Some(HashMap::new()));

    let mut map = HashMap::new();
    map.insert("tx-time".to_string(), "2007-19-01T11:12:00-000".to_string());
    let update = s3.update_s3_object_metadata(
        UPDATE_BUCKET.to_string(),
        "meta-key".to_string(),
        Some(map),
        None,
    );
    assert!(update.await.is_ok());

    let read_obj = s3
        .read_s3_object(
            UPDATE_BUCKET.to_string(),
            "meta-key".to_string(),
            None,
            None,
            None,
        )
        .await;
    assert!(read_obj.is_ok());
    assert_eq!(
        read_obj.unwrap().metadata.unwrap()["tx-time"],
        "2007-19-01T11:12:00-000"
    );
}
