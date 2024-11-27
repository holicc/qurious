use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use arrow::datatypes::{Schema, SchemaRef};

use crate::{impl_logical_plan, logical::plan::LogicalPlan};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DdlStatement {
    CreateMemoryTable(CreateMemoryTable),
    DropTable(DropTable),
}

impl DdlStatement {
    pub fn schema(&self) -> SchemaRef {
        match self {
            DdlStatement::CreateMemoryTable(c) => c.schema(),
            DdlStatement::DropTable(_) => Arc::new(Schema::empty()),
        }
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        match self {
            DdlStatement::CreateMemoryTable(c) => c.children(),
            DdlStatement::DropTable(_) => None,
        }
    }
}

impl Display for DdlStatement {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DdlStatement::CreateMemoryTable(CreateMemoryTable { name, .. }) => {
                write!(f, "CreateMemoryTable: [{}]", name)
            }
            DdlStatement::DropTable(DropTable { name, .. }) => write!(f, "DropTable: [{}]", name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateMemoryTable {
    pub schema: SchemaRef,
    pub name: String,
    pub input: Box<LogicalPlan>,
}

impl_logical_plan!(CreateMemoryTable);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}
