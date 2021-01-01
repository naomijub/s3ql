use s3ql::{bucket::*, read::*, region, s3_client, transact::*};
use std::collections::HashMap;

pub const BUCKET: &'static str = "readObjectsBucket";

async fn client() -> rusoto_s3::S3Client {
    let region = region("us-east-1".to_owned(), "http://localhost:4566".to_owned());
    let s3 = s3_client(region);
    let _ = s3.create_s3_bucket(BUCKET.to_string(), None).await;

    s3
}

async fn insert(s3: &rusoto_s3::S3Client) {
    if !s3
        .has_s3_object(BUCKET.to_string(), "key1".to_string(), None, None, None)
        .await
        .is_ok()
    {
        let mut map = HashMap::new();
        map.insert("tx-time".to_string(), "2007-19-01T11:12:00-000".to_string());
        let body = "{\"hello\": \"world\"}";
        let _ = s3
            .insert_s3_object(
                BUCKET.to_string(),
                Some(map),
                "key1".to_string(),
                Some(body.to_string()),
                None,
            )
            .await;
    }

    if !s3
        .has_s3_object(BUCKET.to_string(), "key2".to_string(), None, None, None)
        .await
        .is_ok()
    {
        let mut map = HashMap::new();
        map.insert("tx-time".to_string(), "2008-19-01T11:12:00-000".to_string());
        let body = "{\"hello\": \"india\"}";
        let _ = s3
            .insert_s3_object(
                BUCKET.to_string(),
                Some(map),
                "key2".to_string(),
                Some(body.to_string()),
                None,
            )
            .await;
    }

    if !s3
        .has_s3_object(BUCKET.to_string(), "key3".to_string(), None, None, None)
        .await
        .is_ok()
    {
        let mut map = HashMap::new();
        map.insert("tx-time".to_string(), "2009-19-01T11:12:00-000".to_string());
        let body = "{\"hello\": \"brasil\"}";
        let _ = s3
            .insert_s3_object(
                BUCKET.to_string(),
                Some(map),
                "key3".to_string(),
                Some(body.to_string()),
                None,
            )
            .await;
    }
}

#[tokio::test]
async fn has_object() {
    let s3 = client().await;
    insert(&s3).await;

    let has_obj1 = s3
        .has_s3_object(BUCKET.to_string(), "key1".to_string(), None, None, None)
        .await;
    assert!(has_obj1.is_ok());

    let has_obj2 = s3
        .has_s3_object(BUCKET.to_string(), "key2".to_string(), None, None, None)
        .await;
    assert!(has_obj2.is_ok());

    let has_obj3 = s3
        .has_s3_object(BUCKET.to_string(), "key3".to_string(), None, None, None)
        .await;
    assert!(has_obj3.is_ok());
}

#[tokio::test]
async fn read_object_body() {
    let s3 = client().await;
    insert(&s3).await;

    let read_obj = s3
        .read_s3_object_body(BUCKET.to_string(), "key1".to_string(), None, None, None)
        .await;
    assert!(read_obj.is_some());
    assert_eq!(read_obj.unwrap(), "{\"hello\": \"world\"}");
}

#[tokio::test]
async fn show_objects() {
    let s3 = client().await;
    insert(&s3).await;

    let objs = s3.show_s3_objects(BUCKET.to_string(), None).await;
    assert!(objs.is_ok());
    assert!(objs.unwrap().contents.unwrap().len() == 3);
}

#[tokio::test]
async fn read_object() {
    let s3 = client().await;
    insert(&s3).await;

    let read_obj = s3
        .read_s3_object(BUCKET.to_string(), "key2".to_string(), None, None, None)
        .await;

    assert!(read_obj.unwrap().e_tag.is_some());
}
