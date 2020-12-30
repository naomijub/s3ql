use std::collections::HashMap;
use rusoto_core::{Region, ByteStream};
use rusoto_s3::{ S3Client, S3, PutObjectRequest};

mod bucket;

use bucket::create_bucket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let region = Region::Custom {
        name: "us-east-1".to_owned(),
        endpoint: "http://localhost:4566".to_owned(),
    };

    let s3_client = S3Client::new(region);
    let bucket_name = "traffic".to_owned();

    match create_bucket(&s3_client, bucket_name.clone()).await {
        Ok(res) => println!("successfully created bucket! resp: {:#?}", res),
        Err(err) => println!("Error creating bucket. err: {:#?}", err),
    };

    let mut map = HashMap::new();
    map.insert("name".to_string(), "test".to_string());

    let put_object = PutObjectRequest {
        bucket: bucket_name.clone(),
        body: Some(ByteStream::from("Hello world".as_bytes().to_vec())),
        key: "Blob".to_string(),
        metadata: Some(map),
        ..Default::default()
    };

    match s3_client.put_object(put_object).await {
        Ok(a) => println!("obj put bucket! resp: {:#?}", a),
        Err(e) => println!("err obj! resp: {:#?}", e),
    };
    

    Ok(())
}