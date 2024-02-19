use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct TableRelation {
    table: Option<Cow<'static, str>>,
    schema: Option<Cow<'static, str>>,
    catalog: Option<Cow<'static, str>>,
}

impl TableRelation {
    pub fn to_quoted_string(&self) -> String {
        if let Some(table) = &self.table {
            table.to_string()
        } else {
            String::new()
        }
    }
}

impl<'a> From<&'a str> for TableRelation {
    fn from(value: &str) -> Self {
        Self {
            table: Some(value.to_owned().into()),
            schema: None,
            catalog: None,
        }
    }
}
