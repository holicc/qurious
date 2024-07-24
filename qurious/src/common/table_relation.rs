use std::{fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableRelation {
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

impl TableRelation {
    fn parse_str(a: &str) -> Self {
        let mut idents = a.split('.').into_iter().collect::<Vec<&str>>();

        match idents.len() {
            1 => TableRelation::Bare {
                table: idents.remove(0).into(),
            },
            2 => TableRelation::Partial {
                schema: idents.remove(0).into(),
                table: idents.remove(0).into(),
            },
            3 => TableRelation::Full {
                catalog: idents.remove(0).into(),
                schema: idents.remove(0).into(),
                table: idents.remove(0).into(),
            },
            _ => TableRelation::Bare { table: a.into() },
        }
    }

    /// Return the fully qualified name of the table
    pub fn to_quanlify_name(&self) -> String {
        match self {
            TableRelation::Bare { table } => table.to_string(),
            TableRelation::Partial { schema, table } => {
                format!("{}.{}", schema, table)
            }
            TableRelation::Full { catalog, schema, table } => {
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
        TableRelation::parse_str(&value)
    }
}

impl From<&str> for TableRelation {
    fn from(value: &str) -> Self {
        TableRelation::parse_str(value)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_table_function() {}
}
