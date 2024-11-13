use crate::error::{Error, Result};
use std::{
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher},
    str::FromStr,
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableRelation {
    relation: ResolvedTableReference,
    is_file_source: bool,
    identify: Option<String>,
}

impl TableRelation {
    fn parse_str(a: &str) -> Self {
        let mut idents = a.split('.').into_iter().collect::<Vec<&str>>();

        let relation = match idents.len() {
            1 => ResolvedTableReference::Bare {
                table: idents.remove(0).into(),
            },
            2 => ResolvedTableReference::Partial {
                schema: idents.remove(0).into(),
                table: idents.remove(0).into(),
            },
            3 => ResolvedTableReference::Full {
                catalog: idents.remove(0).into(),
                schema: idents.remove(0).into(),
                table: idents.remove(0).into(),
            },
            _ => ResolvedTableReference::Bare { table: a.into() },
        };

        Self {
            relation,
            is_file_source: false,
            identify: None,
        }
    }

    pub fn parse_file_path(path: &str) -> Self {
        // generate a unique identify for the file
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let identify = format!("{:x}", hasher.finish());

        Self {
            relation: ResolvedTableReference::Bare { table: path.into() },
            is_file_source: true,
            identify: Some(identify),
        }
    }

    pub fn catalog(&self) -> Option<&str> {
        match &self.relation {
            ResolvedTableReference::Bare { .. } => None,
            ResolvedTableReference::Partial { .. } => None,
            ResolvedTableReference::Full { catalog, .. } => Some(catalog),
        }
    }

    pub fn schema(&self) -> Option<&str> {
        match &self.relation {
            ResolvedTableReference::Bare { .. } => None,
            ResolvedTableReference::Partial { schema, .. } => Some(schema),
            ResolvedTableReference::Full { schema, .. } => Some(schema),
        }
    }

    pub fn table(&self) -> &str {
        match &self.relation {
            ResolvedTableReference::Bare { table } => table,
            ResolvedTableReference::Partial { table, .. } => table,
            ResolvedTableReference::Full { table, .. } => table,
        }
    }

    /// Return the fully qualified name of the table
    pub fn to_quanlify_name(&self) -> String {
        if self.is_file_source {
            let identify = self.identify.as_ref().expect("should have identify");
            return format!("tmp_table({})", &identify[..7]);
        }

        match &self.relation {
            ResolvedTableReference::Bare { table } => table.to_string(),
            ResolvedTableReference::Partial { schema, table } => {
                format!("{}.{}", schema, table)
            }
            ResolvedTableReference::Full { catalog, schema, table } => {
                format!("{}.{}.{}", catalog, schema, table)
            }
        }
    }
}

impl Display for TableRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_quanlify_name().fmt(f)
    }
}

impl From<String> for TableRelation {
    fn from(value: String) -> Self {
        TableRelation::parse_str(&value.to_ascii_lowercase())
    }
}

impl From<&str> for TableRelation {
    fn from(value: &str) -> Self {
        TableRelation::parse_str(value.to_ascii_lowercase().as_str())
    }
}

impl FromStr for TableRelation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(TableRelation::parse_str(s))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResolvedTableReference {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: Arc<str>,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: Arc<str>,
        /// The table name
        table: Arc<str>,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: Arc<str>,
        /// The schema containing the table
        schema: Arc<str>,
        /// The table name
        table: Arc<str>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_relation() {
        let table = TableRelation::from("table".to_string());
        assert_eq!(table.to_quanlify_name(), "table");

        let table = TableRelation::from("schema.table".to_string());
        assert_eq!(table.to_quanlify_name(), "schema.table");

        let table = TableRelation::from("catalog.schema.table".to_string());
        assert_eq!(table.to_quanlify_name(), "catalog.schema.table");
    }

    #[test]
    fn test_parse_file_path() {
        let table = TableRelation::parse_file_path("./tests/testdata/file/case1.csv");
        assert_eq!(table.to_quanlify_name(), "tmp_table(b563e59)");
    }
}
