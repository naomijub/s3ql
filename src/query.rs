use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_s3::{
    S3Client, SelectObjectContentError, SelectObjectContentOutput, SelectObjectContentRequest, S3,
};

#[async_trait]
pub trait Queriable: S3 {
    async fn query_s3_object_content(
        &self,
        query: QueryContent,
        body_compression: CompressionType,
        input_serialization: InputObjectFormat,
        output_serialization: OutputObjectFormat,
    ) -> Result<SelectObjectContentOutput, RusotoError<SelectObjectContentError>>;
}

#[async_trait]
impl Queriable for S3Client {
    async fn query_s3_object_content(
        &self,
        query: QueryContent,
        body_compression: CompressionType,
        input_serialization: InputObjectFormat,
        output_serialization: OutputObjectFormat,
    ) -> Result<SelectObjectContentOutput, RusotoError<SelectObjectContentError>> {
        let expression = query.build().unwrap_or("".to_string());
        let (key, bucket) = query.from.unwrap();

        let mut select = SelectObjectContentRequest {
            key,
            bucket,
            expression,
            expression_type: String::from("SQL"),
            ..SelectObjectContentRequest::default()
        };

        match output_serialization {
            OutputObjectFormat::JSON(delimiter) => {
                select.output_serialization = rusoto_s3::OutputSerialization {
                    csv: None,
                    json: Some(rusoto_s3::JSONOutput {
                        record_delimiter: delimiter,
                    }),
                };
            }
        };

        match input_serialization {
            InputObjectFormat::Parquet => {
                select.input_serialization = rusoto_s3::InputSerialization {
                    csv: None,
                    json: None,
                    parquet: Some(rusoto_s3::ParquetInput {}),
                    compression_type: compression(body_compression),
                }
            }
            InputObjectFormat::JSON(t) => {
                select.input_serialization = rusoto_s3::InputSerialization {
                    csv: None,
                    parquet: None,
                    compression_type: compression(body_compression),
                    json: Some(rusoto_s3::JSONInput {
                        type_: match t {
                            JsonType::Document => Some(String::from("Document")),
                            JsonType::Lines => Some(String::from("Lines")),
                        },
                    }),
                }
            }
        }

        self.select_object_content(select).await
    }
}

fn compression(compression: CompressionType) -> Option<String> {
    match compression {
        CompressionType::BZIP2 => Some("BZIP2".to_string()),
        CompressionType::GZIP => Some("GZIP".to_string()),
        CompressionType::NONE => None,
    }
}

#[derive(Clone)]
pub enum JsonType {
    Document,
    Lines,
}

#[derive(Clone)]
pub enum InputObjectFormat {
    JSON(JsonType),
    Parquet, // CSV,
}

#[derive(Clone)]
pub enum CompressionType {
    NONE,
    GZIP,
    BZIP2,
}

#[derive(Clone)]
pub enum OutputObjectFormat {
    JSON(Option<String>),
    // CSV,
}

#[derive(Clone)]
pub enum Select {
    Elements(Vec<String>),
    Count(String),
    Avg(String),
    Max(String),
    Min(String),
    Sum(String),
}

#[derive(Clone)]
pub enum Clause {
    G(String, usize),
    L(String, usize),
    E(String, String),
    GE(String, usize),
    LE(String, usize),
    NotE(String, String),
    IsNotNull(String),
    IsNull(String),
    Between(String, isize, isize),
    In(String, Vec<String>),
    NotBetween(String, isize, isize),
    NotIn(String, Vec<String>),
    And(Box<Clause>, Box<Clause>),
    Or(Box<Clause>, Box<Clause>),
}

impl Clause {
    fn to_where(self) -> String {
        match self {
            Self::G(n, v) => format!("s.{} > {}", n, v),
            Self::GE(n, v) => format!("s.{} >= {}", n, v),
            Self::L(n, v) => format!("s.{} < {}", n, v),
            Self::LE(n, v) => format!("s.{} <= {}", n, v),
            Self::E(n, v) => format!("s.{} = \"{}\"", n, v),
            Self::NotE(n, v) => format!("s.{} != \"{}\"", n, v),
            Self::IsNotNull(n) => format!("s.{} IS NOT MISSING", n),
            Self::IsNull(n) => format!("s.{} IS MISSING", n),
            Self::In(n, v) => format!(
                "s.{} IN ({})",
                n,
                v.iter()
                    .map(|e| format!("\"{}\"", e))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Self::NotIn(n, v) => format!(
                "s.{} NOT IN ({})",
                n,
                v.iter()
                    .map(|e| format!("\"{}\"", e))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Self::Between(n, start, end) => format!("s.{} BETWEEN {} AND {}", n, start, end),
            Self::NotBetween(n, start, end) => format!("s.{} NOTBETWEEN {} AND {}", n, start, end),
            Self::And(clause1, clause2) => {
                format!("{} AND {}", clause1.to_where(), clause2.to_where())
            }
            Self::Or(clause1, clause2) => {
                format!("{} OR {}", clause1.to_where(), clause2.to_where())
            }
        }
    }
}

#[derive(Clone)]
pub enum Path {
    Index(usize),
    WildCardIndex,
    Name(String),
    WildCardName,
}

#[derive(Clone)]
pub struct QueryContent {
    select: Vec<Select>,            // elements, count, avg, max, min, sum
    from: Option<(String, String)>, // key, bucket
    path: Option<Vec<Path>>,        // S3Object[*].path
    // By name (in an object): .name or ['name']
    // By index (in an array): [index]
    // By wildcard (in an object): .*
    // By wildcard (in an array): [*]
    clauses: Option<Clause>, // >, <, =, "id IS NOT MISSING", between, in, !=, AND, OR, >=, <=
    limit: Option<usize>,
}

impl QueryContent {
    pub fn select(elements: Vec<Select>) -> QueryContent {
        QueryContent {
            select: elements,
            from: None,
            path: None,
            clauses: None,
            limit: None,
        }
    }

    pub fn from(mut self, bucket: &str, key: &str) -> Self {
        self.from = Some((key.to_string(), bucket.to_string()));
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn from_path(mut self, path: Vec<Path>) -> Self {
        self.path = Some(path);
        self
    }

    pub fn where_clause(mut self, clauses: Clause) -> Self {
        self.clauses = Some(clauses);
        self
    }

    fn build(&self) -> Result<String, String> {
        let mut query = String::from("SELECT ");
        if self.from.is_none() {
            return Err("".to_string());
        }

        query = query + &build_select(self.select.clone());
        if self.path.is_none() {
            query = query + " FROM S3Object s";
        } else {
            query = query + &build_path(self.path.clone().unwrap());
        }

        if self.clauses.is_some() {
            query = query + &build_where(self.clauses.clone().unwrap());
        }

        if self.limit.is_some() {
            query = query + " LIMIT " + &self.limit.unwrap().to_string();
        }
        Ok(query)
    }
}

fn build_select(elements: Vec<Select>) -> String {
    elements
        .iter()
        .map(|e| match e {
            Select::Count(el) => build_select_element("Count", el),
            Select::Avg(el) => build_select_element("Avg", el),
            Select::Max(el) => build_select_element("Max", el),
            Select::Min(el) => build_select_element("Min", el),
            Select::Sum(el) => build_select_element("Sum", el),
            Select::Elements(els) => els
                .iter()
                .map(|el| format!("s.{}", el))
                .collect::<Vec<String>>()
                .join(", "),
        })
        .collect::<Vec<String>>()
        .join(", ")
}

fn build_select_element(function: &str, el: &String) -> String {
    if el.contains("*") {
        format!("{}({})", function, el)
    } else {
        format!("{}(s.{})", function, el)
    }
}

fn build_path(path: Vec<Path>) -> String {
    String::from(" FROM S3Object")
        + &path
            .iter()
            .map(|e| match e {
                Path::Index(i) => format!("[{}]", i),
                Path::Name(name) => format!(".{}", name),
                Path::WildCardIndex => String::from("[*]"),
                Path::WildCardName => String::from(".*"),
            })
            .collect::<Vec<String>>()
            .join("")
        + " s"
}

fn build_where(clause: Clause) -> String {
    String::from(" WHERE ") + &clause.to_where()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn build_select() {
        let elements = vec![
            Select::Elements(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
            ]),
            Select::Count("*".to_string()),
            Select::Avg("age".to_string()),
        ];

        let query = QueryContent::select(elements)
            .from("bucket", "key")
            .build()
            .unwrap();

        assert_eq!(
            query,
            "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s"
        );
    }

    #[test]
    fn build_limit() {
        let elements = vec![
            Select::Elements(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
            ]),
            Select::Count("*".to_string()),
            Select::Avg("age".to_string()),
        ];

        let query = QueryContent::select(elements)
            .from("bucket", "key")
            .limit(5)
            .build()
            .unwrap();

        assert_eq!(
            query,
            "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s LIMIT 5"
        );
    }

    #[test]
    fn build_path() {
        let elements = vec![
            Select::Elements(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
            ]),
            Select::Count("*".to_string()),
            Select::Avg("age".to_string()),
        ];

        let query = QueryContent::select(elements)
            .from("bucket", "key")
            .from_path(vec![
                Path::WildCardName,
                Path::Index(5),
                Path::Name("Rules".to_string()),
                Path::WildCardIndex,
            ])
            .build()
            .unwrap();

        assert_eq!(
            query,
            "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object.*[5].Rules[*] s"
        );
    }
}

#[cfg(test)]
mod where_test {
    use super::*;

    fn select() -> QueryContent {
        let elements = vec![
            Select::Elements(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
            ]),
            Select::Count("*".to_string()),
            Select::Avg("age".to_string()),
        ];

        QueryContent::select(elements).from("bucket", "key")
    }

    #[test]
    fn and_greater() {
        let query = select();
        let query_str = query
            .where_clause(Clause::And(
                Box::new(Clause::GE("id".to_string(), 300)),
                Box::new(Clause::G("age".to_string(), 4)),
            ))
            .build()
            .unwrap();

        assert_eq!(query_str, "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s WHERE s.id >= 300 AND s.age > 4");
    }

    #[test]
    fn or_lesser() {
        let query = select();
        let query_str = query
            .where_clause(Clause::Or(
                Box::new(Clause::LE("id".to_string(), 300)),
                Box::new(Clause::L("age".to_string(), 4)),
            ))
            .build()
            .unwrap();

        assert_eq!(query_str, "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s WHERE s.id <= 300 OR s.age < 4");
    }

    #[test]
    fn and_equal() {
        let query = select();
        let query_str = query
            .where_clause(Clause::And(
                Box::new(Clause::E("id".to_string(), "74927".to_string())),
                Box::new(Clause::NotE("name".to_string(), "test".to_string())),
            ))
            .build()
            .unwrap();

        assert_eq!(query_str, "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s WHERE s.id = \"74927\" AND s.name != \"test\"");
    }

    #[test]
    fn or_missing() {
        let query = select();
        let query_str = query
            .where_clause(Clause::Or(
                Box::new(Clause::IsNotNull("id".to_string())),
                Box::new(Clause::IsNull("name".to_string())),
            ))
            .build()
            .unwrap();

        assert_eq!(query_str, "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s WHERE s.id IS NOT MISSING OR s.name IS MISSING");
    }

    #[test]
    fn and_in() {
        let query = select();
        let query_str = query
            .where_clause(Clause::And(
                Box::new(Clause::In(
                    "name".to_string(),
                    vec!["julia".to_string(), "naomi".to_string()],
                )),
                Box::new(Clause::NotIn(
                    "id".to_string(),
                    vec!["432904".to_string(), "90jd243".to_string()],
                )),
            ))
            .build()
            .unwrap();

        assert_eq!(query_str, "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s WHERE s.name IN (\"julia\", \"naomi\") AND s.id NOT IN (\"432904\", \"90jd243\")");
    }

    #[test]
    fn betweens() {
        let query = select();
        let query_str = query
            .where_clause(Clause::And(
                Box::new(Clause::Between("age".to_string(), 25, 35)),
                Box::new(Clause::NotBetween("id".to_string(), 300, 500)),
            ))
            .build()
            .unwrap();

        assert_eq!(query_str, "SELECT s.id, s.name, s.age, Count(*), Avg(s.age) FROM S3Object s WHERE s.age BETWEEN 25 AND 35 AND s.id NOTBETWEEN 300 AND 500");
    }
}
