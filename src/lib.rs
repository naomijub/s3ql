pub mod bucket;
pub mod query;
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

/// `s3_client_with` allows user to configure credentials and request dispatcher for AWS S3 client;
#[cfg(feature = "auth")]
pub fn s3_client_with<P, D>(
    request_dispatcher: D,
    credentials_provider: P,
    region: Region,
) -> S3Client
where
    P: rusoto_credential::ProvideAwsCredentials + Send + Sync + 'static,
    P::Future: Send,
    D: rusoto_credential::DispatchSignedRequest + Send + Sync + 'static,
    D::Future: Send,
{
    S3Client::new_with(request_dispatcher, credentials_provider, region)
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
