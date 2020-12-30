pub mod bucket;
use rusoto_core::{Region};
use rusoto_s3::{ S3Client};

pub fn region(name: String, endpoint: String) -> Region {
    Region::Custom {
        name: name,
        endpoint: endpoint,
    }
}

pub fn s3_client(region: Region) -> S3Client {
    S3Client::new(region)
}