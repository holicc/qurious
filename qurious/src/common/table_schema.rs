use std::{collections::HashSet, fmt::Display, sync::Arc};

use super::table_relation::TableRelation;
use crate::{
    error::{Error, Result},
    internal_err,
    logical::expr::Column,
};
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};

pub type TableSchemaRef = Arc<TableSchema>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableSchema {
    pub schema: SchemaRef,
    pub field_qualifiers: Vec<Option<TableRelation>>,
}

impl TableSchema {
    pub fn try_new(qualified_fields: Vec<(Option<TableRelation>, Arc<Field>)>) -> Result<Self> {
        let (qualifiers, fields): (Vec<_>, Vec<_>) = qualified_fields.into_iter().unzip();
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            field_qualifiers: qualifiers,
        })
    }

    /// Deprecated use `try_new` instead
    pub fn new(field_qualifiers: Vec<Option<TableRelation>>, schema: SchemaRef) -> Self {
        Self {
            field_qualifiers,
            schema,
        }
    }

    pub fn try_from_qualified_schema(relation: impl Into<TableRelation>, schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            field_qualifiers: vec![Some(relation.into()); schema.fields().len()],
            schema,
        })
    }

    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            field_qualifiers: vec![],
        }
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn has_field(&self, qualifier: Option<TableRelation>, name: &str) -> bool {
        match (self.schema.index_of(name).ok(), qualifier) {
            (Some(i), Some(q)) => self.field_qualifiers[i] == Some(q),
            (Some(_), None) => true,
            _ => false,
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        self.schema
            .fields()
            .iter()
            .zip(self.field_qualifiers.iter())
            .map(|(f, q)| Column::new(f.name(), q.clone(), false))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Option<&TableRelation>, &FieldRef)> {
        self.field_qualifiers
            .iter()
            .zip(self.schema.fields().iter())
            .map(|(q, f)| (q.as_ref(), f))
    }
}

impl TableSchema {
    pub fn merge(schemas: Vec<TableSchemaRef>) -> Result<Self> {
        let fields = schemas
            .into_iter()
            .map(|s| {
                let s = Arc::unwrap_or_clone(s);
                let fields = s.schema.fields().iter().map(|f| f.as_ref().clone());
                let field_qualifiers = s.field_qualifiers.into_iter();
                fields.zip(field_qualifiers).collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();

        // check if the number of fields and qualifiers are the same
        let mut new_fields = HashSet::new();
        for (f, q) in &fields {
            if !new_fields.insert((f, q)) {
                return internal_err!(
                    "Try merge schema failed, column [{}] is ambiguous, please use qualified name to disambiguate",
                    f.name()
                );
            }
        }

        let (fields, field_qualifiers): (Vec<_>, Vec<_>) = fields.into_iter().unzip();

        Ok(TableSchema {
            schema: Arc::new(Schema::new(fields)),
            field_qualifiers,
        })
    }
}

impl Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.schema
                .fields()
                .iter()
                .zip(self.field_qualifiers.iter())
                .map(|(f, q)| qualified_name(q, &f.name()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl From<SchemaRef> for TableSchema {
    fn from(value: SchemaRef) -> Self {
        TableSchema {
            schema: value,
            field_qualifiers: vec![],
        }
    }
}

impl From<Schema> for TableSchema {
    fn from(value: Schema) -> Self {
        TableSchema::from(Arc::new(value))
    }
}

pub fn qualified_name(qualifier: &Option<TableRelation>, name: &str) -> String {
    match qualifier {
        Some(q) => format!("{}.{}", q, name),
        None => name.to_string(),
    }
}
