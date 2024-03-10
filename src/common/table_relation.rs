use std::{borrow::Cow, fmt::Display};

pub type OwnedTableRelation = TableRelation<'static>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableRelation<'a> {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: Cow<'a, str>,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: Cow<'a, str>,
        /// The table name
        table: Cow<'a, str>,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: Cow<'a, str>,
        /// The schema containing the table
        schema: Cow<'a, str>,
        /// The table name
        table: Cow<'a, str>,
    },
}

impl<'a> TableRelation<'a> {
    pub fn to_owned(&self) -> OwnedTableRelation {
        match self {
            TableRelation::Bare { table } => OwnedTableRelation::Bare {
                table: table.to_string().into(),
            },
            TableRelation::Partial { schema, table } => OwnedTableRelation::Partial {
                schema: schema.to_string().into(),
                table: table.to_string().into(),
            },
            TableRelation::Full {
                catalog,
                schema,
                table,
            } => OwnedTableRelation::Full {
                catalog: catalog.to_string().into(),
                schema: schema.to_string().into(),
                table: table.to_string().into(),
            },
        }
    }

    fn parse_str(a: &'a str) -> Self {
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
            TableRelation::Full {
                catalog,
                schema,
                table,
            } => {
                format!("{}.{}.{}", catalog, schema, table)
            }
        }
    }
}

impl Display for TableRelation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_quanlify_name().fmt(f)
    }
}

impl<'a> From<&'a str> for TableRelation<'a> {
    fn from(value: &'a str) -> Self {
        TableRelation::parse_str(value)
    }
}

impl From<String> for OwnedTableRelation {
    fn from(value: String) -> Self {
        TableRelation::parse_str(&value).to_owned()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_table_function() {}
}
