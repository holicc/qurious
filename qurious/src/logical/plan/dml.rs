use std::fmt::{self, Display, Formatter};

use arrow::datatypes::SchemaRef;

use crate::{common::table_relation::TableRelation, impl_logical_plan, logical::plan::LogicalPlan};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DmlOperator {
    Insert,
    Update,
    Delete,
}

impl Display for DmlOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DmlOperator::Insert => write!(f, "INSERT"),
            DmlOperator::Update => write!(f, "UPDATE"),
            DmlOperator::Delete => write!(f, "DELETE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DmlStatement {
    pub relation: TableRelation,
    pub op: DmlOperator,
    pub schema: SchemaRef,
    pub input: Box<LogicalPlan>,
}

impl_logical_plan!(DmlStatement);

impl Display for DmlStatement {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.op {
            DmlOperator::Insert => write!(f, "Dml: op=[Insert Into] table=[{}]", self.relation),
            DmlOperator::Delete => write!(f, "Dml: op=[Delete From] table=[{}]", self.relation),
            DmlOperator::Update => write!(f, "Dml: op=[Update] table=[{}]", self.relation),
        }
    }
}
