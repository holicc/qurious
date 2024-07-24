use std::fmt::{self, Display, Formatter};

use arrow::datatypes::SchemaRef;

use crate::{common::table_relation::TableRelation, logical::plan::LogicalPlan};

#[derive(Debug, Clone)]
pub enum DmlStatement {
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

impl DmlStatement {
    pub fn schema(&self) -> SchemaRef {
        match self {
            DmlStatement::Insert(i) => i.input.schema(),
            DmlStatement::Update(u) => u.input.schema(),
            DmlStatement::Delete(d) => d.input.schema(),
        }
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        match self {
            DmlStatement::Insert(i) => Some(vec![&i.input]),
            DmlStatement::Update(u) => Some(vec![&u.input, &u.selection]),
            DmlStatement::Delete(d) => Some(vec![&d.input]),
        }
    }
}

impl Display for DmlStatement {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DmlStatement::Insert(i) => write!(f, "Dml: op=[Insert Into] table=[{}]", i.table_name),
            DmlStatement::Delete(d) => write!(f, "Dml: op=[Delete From] table=[{}]", d.relation),
            DmlStatement::Update(u) => write!(f, "Dml: op=[Update] table=[{}]", u.table_name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Insert {
    pub table_name: String,
    pub table_schema: SchemaRef,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct Update {
    pub relation: TableRelation,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct Delete {
    pub relation: TableRelation,
    pub input: Box<LogicalPlan>,
}
