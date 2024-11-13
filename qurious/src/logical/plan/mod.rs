mod aggregate;
mod ddl;
mod dml;
mod filter;
mod join;
mod limit;
mod projection;
mod scan;
mod sort;
mod sub_query;

use std::fmt::{self, Display, Formatter};

pub use aggregate::Aggregate;
pub use ddl::*;
pub use dml::*;
pub use filter::Filter;
pub use join::*;
pub use limit::Limit;
pub use projection::Projection;
pub use scan::TableScan;
pub use sort::*;
pub use sub_query::SubqueryAlias;

use arrow::datatypes::SchemaRef;

use super::expr::LogicalExpr;
use crate::common::table_relation::TableRelation;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::error::Result;

#[macro_export]
macro_rules! impl_logical_plan {
    ($name:ident) => {
        impl $name {
            pub fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }

            pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
                Some(vec![&self.input])
            }
        }
    };
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Join(Join),
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    TableScan(TableScan),
    EmptyRelation(EmptyRelation),
    /// VALUES (1, 2), (3, 4)
    Values(Values),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Sort the result set by the specified expressions.
    Sort(Sort),
    /// Limit the number of rows in the result set, and optionally an offset.
    Limit(Limit),
    /// Data Definition Language (DDL) statements. CREATE, DROP, etc.
    Ddl(DdlStatement),
    /// Data Manipulation Language (DML) statements. INSERT, UPDATE, DELETE, etc.
    Dml(DmlStatement),
}

impl LogicalPlan {
    pub fn relation(&self) -> Option<TableRelation> {
        match self {
            LogicalPlan::TableScan(s) => Some(s.relation.clone()),
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => Some(alias.clone()),
            _ => None,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Projection(p) => p.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Aggregate(a) => a.schema(),
            LogicalPlan::TableScan(t) => t.schema(),
            LogicalPlan::EmptyRelation(e) => e.schema.clone(),
            LogicalPlan::SubqueryAlias(s) => s.schema(),
            LogicalPlan::Join(j) => j.schema(),
            LogicalPlan::Sort(s) => s.schema(),
            LogicalPlan::Limit(l) => l.schema(),
            LogicalPlan::Ddl(d) => d.schema(),
            LogicalPlan::Dml(d) => d.schema(),
            LogicalPlan::Values(v) => v.schema.clone(),
        }
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        match self {
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => None,
            LogicalPlan::Projection(p) => p.children(),
            LogicalPlan::Filter(f) => f.children(),
            LogicalPlan::Aggregate(a) => a.children(),
            LogicalPlan::TableScan(t) => t.children(),
            LogicalPlan::SubqueryAlias(s) => s.children(),
            LogicalPlan::Join(j) => j.children(),
            LogicalPlan::Sort(s) => s.children(),
            LogicalPlan::Limit(l) => l.children(),
            LogicalPlan::Ddl(l) => l.children(),
            LogicalPlan::Dml(l) => l.children(),
        }
    }

    pub fn map_exprs<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(LogicalExpr) -> Result<Transformed<LogicalExpr>>,
    {
        match self {
            LogicalPlan::Projection(Projection { schema, input, exprs }) => exprs
                .into_iter()
                .map(|expr| f(expr).data())
                .collect::<Result<Vec<_>>>()
                .map(|exprs| Transformed::yes(LogicalPlan::Projection(Projection { schema, input, exprs }))),
            LogicalPlan::Aggregate(Aggregate {
                schema,
                input,
                group_expr,
                aggr_expr,
            }) => {
                let group_expr = group_expr
                    .into_iter()
                    .map(|expr| f(expr).data())
                    .collect::<Result<Vec<_>>>()?;
                let aggr_expr = aggr_expr
                    .into_iter()
                    .map(|expr| f(expr).data())
                    .collect::<Result<Vec<_>>>()?;

                Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate {
                    schema,
                    input,
                    group_expr,
                    aggr_expr,
                })))
            }
            _ => Ok(Transformed::no(self)),
        }
    }
}

impl TransformNode for LogicalPlan {
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(self, mut f: F) -> Result<Transformed<Self>> {
        Ok(match self {
            LogicalPlan::Projection(Projection { schema, input, exprs }) => f(*input)?.update(|input| {
                LogicalPlan::Projection(Projection {
                    schema,
                    input: Box::new(input),
                    exprs,
                })
            }),
            LogicalPlan::Aggregate(Aggregate {
                schema,
                input,
                group_expr,
                aggr_expr,
            }) => f(*input)?.update(|input| {
                LogicalPlan::Aggregate(Aggregate {
                    schema,
                    input: Box::new(input),
                    group_expr,
                    aggr_expr,
                })
            }),
            LogicalPlan::Sort(Sort { exprs, input }) => f(*input)?.update(|input| {
                LogicalPlan::Sort(Sort {
                    exprs,
                    input: Box::new(input),
                })
            }),
            _ => Transformed::no(self),
        })
    }

    fn apply_children<'n, F>(&'n self, _f: F) -> Result<crate::common::transformed::TreeNodeRecursion>
    where
        F: FnMut(&'n Self) -> Result<crate::common::transformed::TreeNodeRecursion>,
    {
        todo!()
    }
}

pub fn base_plan(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Aggregate(Aggregate { input, .. }) => base_plan(&input),
        _ => plan,
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
            LogicalPlan::SubqueryAlias(s) => write!(f, "{}", s),
            LogicalPlan::Join(j) => write!(f, "{}", j),
            LogicalPlan::Sort(s) => write!(f, "{}", s),
            LogicalPlan::Limit(l) => write!(f, "{}", l),
            LogicalPlan::Ddl(l) => write!(f, "{}", l),
            LogicalPlan::Values(v) => write!(f, "{}", v),
            LogicalPlan::Dml(d) => write!(f, "{}", d),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyRelation {
    pub produce_one_row: bool,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone)]
pub struct Values {
    pub values: Vec<Vec<LogicalExpr>>,
    pub schema: SchemaRef,
}

impl Display for Values {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Values: [")?;
        for (i, row) in self.values.iter().enumerate() {
            write!(f, "[")?;
            for (j, value) in row.iter().enumerate() {
                write!(f, "{}", value)?;
                if j < row.len() - 1 {
                    write!(f, ", ")?;
                }
            }
            write!(f, "]")?;
            if i < self.values.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}
