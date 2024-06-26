mod aggregate;
mod filter;
mod join;
mod limit;
mod projection;
mod scan;
mod sort;
mod sub_query;

pub use aggregate::Aggregate;
pub use filter::Filter;
pub use join::*;
pub use limit::Limit;
pub use projection::Projection;
pub use scan::TableScan;
pub use sort::*;
pub use sub_query::SubqueryAlias;

use arrow::datatypes::SchemaRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyRelation {
    pub produce_one_row: bool,
    pub schema: SchemaRef,
}

impl EmptyRelation {
    pub fn new(schema: SchemaRef, produce_one_row: bool) -> Self {
        Self {
            produce_one_row,
            schema,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        None
    }
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Apply Cross Join to two logical plans.
    CrossJoin(CrossJoin),
    Join(Join),
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    TableScan(TableScan),
    EmptyRelation(EmptyRelation),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Sort the result set by the specified expressions.
    Sort(Sort),
    /// Limit the number of rows in the result set, and optionally an offset.
    Limit(Limit),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Projection(p) => p.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Aggregate(a) => a.schema(),
            LogicalPlan::TableScan(t) => t.schema(),
            LogicalPlan::EmptyRelation(e) => e.schema(),
            LogicalPlan::CrossJoin(s) => s.schema(),
            LogicalPlan::SubqueryAlias(s) => s.schema(),
            LogicalPlan::Join(j) => j.schema(),
            LogicalPlan::Sort(s) => s.schema(),
            LogicalPlan::Limit(l) => l.schema(),
        }
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        match self {
            LogicalPlan::Projection(p) => p.children(),
            LogicalPlan::Filter(f) => f.children(),
            LogicalPlan::Aggregate(a) => a.children(),
            LogicalPlan::TableScan(t) => t.children(),
            LogicalPlan::EmptyRelation(e) => e.children(),
            LogicalPlan::CrossJoin(s) => s.children(),
            LogicalPlan::SubqueryAlias(s) => s.children(),
            LogicalPlan::Join(j) => j.children(),
            LogicalPlan::Sort(s) => s.children(),
            LogicalPlan::Limit(l) => l.children(),
        }
    }
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalPlan::Projection(p) => write!(f, "{}", p),
            LogicalPlan::Filter(a) => write!(f, "{}", a),
            LogicalPlan::Aggregate(a) => write!(f, "{}", a),
            LogicalPlan::TableScan(t) => write!(f, "{}", t),
            LogicalPlan::EmptyRelation(_) => write!(f, "Empty Relation"),
            LogicalPlan::CrossJoin(s) => write!(f, "{}", s),
            LogicalPlan::SubqueryAlias(s) => write!(f, "{}", s),
            LogicalPlan::Join(j) => write!(f, "{}", j),
            LogicalPlan::Sort(s) => write!(f, "{}", s),
            LogicalPlan::Limit(l) => write!(f, "{}", l),
        }
    }
}
