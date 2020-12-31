pub mod bucket;
pub mod read;
pub mod transact;

use rusoto_core::Region;
use rusoto_s3::S3Client;

pub fn region(name: String, endpoint: String) -> Region {
    Region::Custom { name, endpoint }
}

pub fn s3_client(region: Region) -> S3Client {
    S3Client::new(region)
}

#[test]
fn region_test() {
    let actual = region("name".to_string(), "endpoint".to_string());
    let expected = Region::Custom {
        name: "name".to_string(),
        endpoint: "endpoint".to_string(),
    };

    assert_eq!(actual, expected);
}
