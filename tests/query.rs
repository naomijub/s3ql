use s3ql::{bucket::*, query::*, region, s3_client, transact::*};

fn client() -> rusoto_s3::S3Client {
    let region = region("us-east-1".to_owned(), "http://localhost:4566".to_owned());
    s3_client(region)
}

pub const BUCKET: &'static str = "selectObjectsBucket";

#[ignore] // Only runs on localstack pro and aws. Any issues PLEASE REPORT
#[tokio::test]
async fn select_object() {
    let s3 = client();
    let bucket = s3.create_s3_bucket(BUCKET.to_string(), None).await;

    assert!(bucket.is_ok());

    let body = "{\"name\": \"world\"}
{\"name\": \"india\", \"count\":1000}
{\"name\": \"china\", \"count\":1300}
{\"name\": \"ghana\"}
{\"hello\": \"brasil\", \"count\":200}";

    let insert = s3.insert_s3_object(
        BUCKET.to_string(),
        None,
        "select-key".to_string(),
        Some(body.to_string()),
        None,
    );

    assert!(insert.await.is_ok());
    let query = QueryContent::select(vec![Select::Elements(vec!["name".to_string()])])
        .from(BUCKET, "select-key")
        .limit(2)
        .where_clause(Clause::IsNotNull("count".to_string()));

    let select = s3
        .query_s3_object_content(
            query,
            CompressionType::NONE,
            InputObjectFormat::JSON(JsonType::Document),
            OutputObjectFormat::JSON(Some(",".to_string())),
        )
        .await;

    println!("{:?}", select);

    assert!(select.is_ok());
}
