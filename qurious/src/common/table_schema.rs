use std::{collections::HashSet, fmt::Display, sync::Arc};

use super::table_relation::TableRelation;
use crate::{
    error::{Error, Result},
    internal_err,
    logical::expr::Column,
};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};

pub type TableSchemaRef = Arc<TableSchema>;

/// Arrow schema metadata key used to preserve per-field qualifiers (table/alias) across planning stages.
///
/// This is needed because Arrow `Schema` fields are identified by name only, and we allow duplicate
/// column names across different relations (e.g. `nation n1`, `nation n2` both have `n_name`).
/// Physical planning must be able to map a `(relation, column_name)` to the correct field index.
pub const FIELD_QUALIFIERS_META_KEY: &str = "qurious.field_qualifiers";
const FIELD_QUALIFIERS_META_SEP: char = '\u{1f}'; // unit separator (unlikely to appear in names)

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

    /// Deprecated, use `try_new` instead.
    ///
    /// Pads `field_qualifiers` out to the field count. `iter` zips the two, so a list shorter than
    /// the schema silently makes the trailing fields invisible -- an empty one hides every field.
    pub fn new(mut field_qualifiers: Vec<Option<TableRelation>>, schema: SchemaRef) -> Self {
        field_qualifiers.resize(schema.fields().len(), None);

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
        // Preserve qualifier information via Schema metadata so physical planning can disambiguate
        // same-named fields from different relations.
        let mut metadata = self.schema.metadata().clone();
        let qualifiers = self
            .field_qualifiers
            .iter()
            .map(|q| q.as_ref().map(|t| t.to_qualified_name()).unwrap_or_default())
            .collect::<Vec<_>>()
            .join(&FIELD_QUALIFIERS_META_SEP.to_string());
        metadata.insert(FIELD_QUALIFIERS_META_KEY.to_string(), qualifiers);

        let fields = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        Arc::new(Schema::new_with_metadata(fields, metadata))
    }

    /// Whether this schema holds a field with the given qualifier and name.
    ///
    /// Every field of that name has to be considered, not just the first: a joined schema can hold
    /// several columns sharing a name, and looking only at the first reports `false` for all the
    /// others. Callers use this to decide which side of a join a predicate belongs to, so a false
    /// negative silently reclassifies -- or drops -- the predicate.
    pub fn has_field(&self, qualifier: Option<&TableRelation>, name: &str) -> bool {
        self.iter()
            .any(|(field_qualifier, field)| field.name() == name && field_qualifier == qualifier)
    }

    pub fn has_column(&self, column: &Column) -> bool {
        self.has_field(column.relation.as_ref(), &column.name)
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

    pub fn data_type_and_nullable(&self, relation: Option<&TableRelation>, name: &str) -> Result<(DataType, bool)> {
        let index = self
            .schema
            .index_of(name)
            .map_err(|e| Error::InternalError(format!("Field [{}] not found in schema, error: {}", name, e)))?;

        if self.field_qualifiers[index].as_ref() != relation {
            return internal_err!("Field [{}] is not qualified with [{}]", name, relation.unwrap());
        }

        let field = self.schema.field(index);

        Ok((field.data_type().clone(), field.is_nullable()))
    }

    pub fn qualified_field(&self, index: usize) -> (Option<&TableRelation>, &Field) {
        (self.field_qualifiers[index].as_ref(), self.schema.field(index))
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
                .map(|(f, q)| qualified_name(q.as_ref(), &f.name()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl From<SchemaRef> for TableSchema {
    /// Recovers the qualifiers `arrow_schema` recorded in the schema's metadata.
    ///
    /// Without this the round trip through an arrow `Schema` is lossy and silently unqualifies
    /// every column, so two relations' same-named columns become indistinguishable again.
    ///
    /// `field_qualifiers` is always as long as the field list, since `iter` zips the two and a
    /// short list makes the trailing fields invisible.
    fn from(value: SchemaRef) -> Self {
        let recorded = value
            .metadata()
            .get(FIELD_QUALIFIERS_META_KEY)
            .map(|qualifiers| {
                qualifiers
                    .split(FIELD_QUALIFIERS_META_SEP)
                    .map(|qualifier| (!qualifier.is_empty()).then(|| TableRelation::from(qualifier)))
                    .collect::<Vec<_>>()
            })
            .filter(|qualifiers| qualifiers.len() == value.fields().len());

        TableSchema {
            field_qualifiers: recorded.unwrap_or_else(|| vec![None; value.fields().len()]),
            schema: value,
        }
    }
}

impl From<Schema> for TableSchema {
    fn from(value: Schema) -> Self {
        TableSchema::from(Arc::new(value))
    }
}

/// Index of the field a `(qualifier, name)` pair refers to in an arrow schema.
///
/// Arrow identifies fields by name alone, so a schema holding columns from several relations can
/// contain the same name more than once. The qualifiers are carried alongside in schema metadata
/// under [`FIELD_QUALIFIERS_META_KEY`]; consult them so the right one is found, and fall back to
/// the name when there is no qualifier to match on.
pub fn qualified_field_index(schema: &Schema, qualifier: Option<&TableRelation>, name: &str) -> Option<usize> {
    if let (Some(qualifier), Some(recorded)) = (qualifier, schema.metadata().get(FIELD_QUALIFIERS_META_KEY)) {
        let wanted = qualifier.to_qualified_name();
        let qualifiers = recorded.split(FIELD_QUALIFIERS_META_SEP).collect::<Vec<_>>();

        if qualifiers.len() == schema.fields().len() {
            let found = schema
                .fields()
                .iter()
                .enumerate()
                .find(|(index, field)| field.name() == name && qualifiers[*index] == wanted);

            if let Some((index, _)) = found {
                return Some(index);
            }
        }
    }

    schema.index_of(name).ok()
}

pub fn qualified_name(qualifier: Option<&TableRelation>, name: &str) -> String {
    match qualifier {
        Some(q) => format!("{}.{}", q, name),
        None => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qualified(relation: &str, name: &str) -> (Option<TableRelation>, Arc<Field>) {
        (
            Some(TableRelation::from(relation)),
            Arc::new(Field::new(name, DataType::Int64, true)),
        )
    }

    /// A joined schema can hold several columns sharing a name. `has_field` used to look at only
    /// the first of them, so every later one was reported missing; callers use it to decide which
    /// side of a join a predicate belongs to, so that silently misplaced or dropped the predicate.
    #[test]
    fn has_field_finds_every_column_of_a_shared_name() {
        let schema = TableSchema::try_new(vec![
            qualified("li", "pk"),
            qualified("li", "qty"),
            qualified("pt", "pk"),
            qualified("pt", "brand"),
        ])
        .unwrap();

        assert!(schema.has_field(Some(&"li".into()), "pk"));
        assert!(
            schema.has_field(Some(&"pt".into()), "pk"),
            "the second `pk` was not found"
        );
        assert!(schema.has_field(Some(&"li".into()), "qty"));
        assert!(schema.has_field(Some(&"pt".into()), "brand"));

        assert!(!schema.has_field(Some(&"other".into()), "pk"));
        assert!(!schema.has_field(Some(&"li".into()), "brand"));
        assert!(!schema.has_field(None, "pk"));
    }

    #[test]
    fn qualifiers_survive_a_round_trip_through_an_arrow_schema() {
        // `arrow_schema` records the qualifiers in metadata; rebuilding a TableSchema from it has
        // to read them back, or two relations' same-named columns become indistinguishable again.
        let original = TableSchema::try_new(vec![
            qualified("li", "pk"),
            qualified("li", "v"),
            qualified("pt", "pk"),
            (None, Arc::new(Field::new("bare", DataType::Int64, true))),
        ])
        .unwrap();

        let round_tripped = TableSchema::from(original.arrow_schema());

        assert_eq!(round_tripped.field_qualifiers, original.field_qualifiers);
        assert!(round_tripped.has_field(Some(&"pt".into()), "pk"));
        assert!(round_tripped.has_field(None, "bare"));
    }

    #[test]
    fn a_schema_without_qualifier_metadata_is_fully_unqualified() {
        let plain = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));

        let schema = TableSchema::from(plain);

        // Not an empty vec: `iter` zips qualifiers with fields, so a short list hides fields.
        assert_eq!(schema.field_qualifiers, vec![None, None]);
        assert_eq!(schema.iter().count(), 2);
        assert!(schema.has_field(None, "a"));
    }

    #[test]
    fn new_pads_a_short_qualifier_list() {
        let plain = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));

        let schema = TableSchema::new(vec![], plain);

        assert_eq!(schema.iter().count(), 2, "an empty qualifier list hid every field");
    }

    #[test]
    fn qualified_field_index_picks_the_right_side_of_a_join() {
        let joined = TableSchema::try_new(vec![
            qualified("da", "k"),
            qualified("da", "v"),
            qualified("db", "k"),
            qualified("db", "v"),
        ])
        .unwrap()
        .arrow_schema();

        assert_eq!(qualified_field_index(&joined, Some(&"da".into()), "v"), Some(1));
        assert_eq!(qualified_field_index(&joined, Some(&"db".into()), "v"), Some(3));
        assert_eq!(
            qualified_field_index(&joined, Some(&"nope".into()), "v"),
            Some(1),
            "falls back to the name"
        );
        assert_eq!(qualified_field_index(&joined, None, "v"), Some(1));
        assert_eq!(qualified_field_index(&joined, Some(&"da".into()), "missing"), None);
    }

    #[test]
    fn has_field_matches_unqualified_fields() {
        let schema = TableSchema::try_new(vec![
            (None, Arc::new(Field::new("bare", DataType::Int64, true))),
            qualified("t", "named"),
        ])
        .unwrap();

        assert!(schema.has_field(None, "bare"));
        assert!(!schema.has_field(Some(&"t".into()), "bare"));
        assert!(schema.has_field(Some(&"t".into()), "named"));
        assert!(!schema.has_field(None, "named"));
    }
}
