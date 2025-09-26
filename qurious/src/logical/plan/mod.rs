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
use std::sync::Arc;

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

use super::expr::{Column, LogicalExpr};
use crate::common::table_relation::TableRelation;
use crate::common::table_schema::TableSchemaRef;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult, TreeNodeContainer, TreeNodeRecursion};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalPlan {
    /// Apply Cross Join to two logical plans.
    CrossJoin(CrossJoin),
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
    pub fn head_output_expr(&self) -> Result<Option<LogicalExpr>> {
        match self {
            LogicalPlan::Projection(p) => Ok(p.exprs.first().cloned()),
            _ => todo!("[{}] not implement head_output_expr", self),
        }
    }

    pub fn outer_ref_columns(&self) -> Result<Vec<LogicalExpr>> {
        let mut outer_ref_columns = vec![];

        let mut stack = vec![self];

        while let Some(plan) = stack.pop() {
            match plan.apply_exprs(|expr| {
                expr.apply_children(|expr| {
                    match expr {
                        LogicalExpr::Column(Column { is_outer_ref: true, .. }) => {
                            outer_ref_columns.push(expr.clone());
                        }
                        _ => {}
                    }

                    Ok(TreeNodeRecursion::Continue)
                })
            })? {
                TreeNodeRecursion::Continue => {
                    if let Some(children) = plan.children() {
                        stack.extend(children);
                    }
                }
                TreeNodeRecursion::Stop => return Ok(outer_ref_columns),
            }
        }

        Ok(outer_ref_columns)
    }

    pub fn relation(&self) -> Option<TableRelation> {
        match self {
            LogicalPlan::TableScan(s) => Some(s.table_name.clone()),
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => Some(alias.clone()),
            _ => None,
        }
    }

    // FIXME: remove this method when table schema is implemented
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Projection(p) => p.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Aggregate(a) => a.schema().arrow_schema(),
            LogicalPlan::TableScan(t) => t.schema(),
            LogicalPlan::EmptyRelation(e) => e.schema.clone(),
            LogicalPlan::CrossJoin(s) => s.schema(),
            LogicalPlan::SubqueryAlias(s) => s.schema(),
            LogicalPlan::Join(j) => j.schema(),
            LogicalPlan::Sort(s) => s.schema(),
            LogicalPlan::Limit(l) => l.schema(),
            LogicalPlan::Ddl(d) => d.schema(),
            LogicalPlan::Dml(d) => d.schema(),
            LogicalPlan::Values(v) => v.schema.clone(),
        }
    }

    pub fn table_schema(&self) -> TableSchemaRef {
        match self {
            LogicalPlan::TableScan(s) => s.schema.clone(),
            LogicalPlan::CrossJoin(s) => s.schema.clone(),
            LogicalPlan::SubqueryAlias(s) => s.schema.clone(),
            LogicalPlan::Filter(f) => f.input.table_schema(),
            LogicalPlan::Projection(p) => p.schema.clone(),
            LogicalPlan::Join(j) => j.schema.clone(),
            LogicalPlan::Aggregate(a) => a.schema.clone(),
            _ => todo!("[{}] not implement table_schema", self),
        }
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        match self {
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => None,
            LogicalPlan::Projection(p) => p.children(),
            LogicalPlan::Filter(f) => f.children(),
            LogicalPlan::Aggregate(a) => a.children(),
            LogicalPlan::TableScan(t) => t.children(),
            LogicalPlan::CrossJoin(s) => s.children(),
            LogicalPlan::SubqueryAlias(s) => s.children(),
            LogicalPlan::Join(j) => j.children(),
            LogicalPlan::Sort(s) => s.children(),
            LogicalPlan::Limit(l) => l.children(),
            LogicalPlan::Ddl(l) => l.children(),
            LogicalPlan::Dml(l) => l.children(),
        }
    }

    pub fn apply_exprs<F>(&self, mut f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&LogicalExpr) -> Result<TreeNodeRecursion>,
    {
        match self {
            LogicalPlan::Projection(Projection { exprs, .. }) => exprs.apply(f),
            LogicalPlan::Aggregate(Aggregate {
                group_expr, aggr_expr, ..
            }) => {
                group_expr.apply(&mut f)?;
                aggr_expr.apply(&mut f)?;

                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::Filter(Filter { expr, .. }) => f(expr),
            _ => Ok(TreeNodeRecursion::Continue),
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
            LogicalPlan::Filter(Filter { input, expr }) => Ok(Transformed::yes(LogicalPlan::Filter(Filter {
                input,
                expr: f(expr).data()?,
            }))),
            _ => Ok(Transformed::no(self)),
        }
    }
}

impl TransformNode for LogicalPlan {
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(self, mut f: F) -> Result<Transformed<Self>> {
        Ok(match self {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, schema }) => f(Arc::unwrap_or_clone(input))?
                .update(|input| {
                    LogicalPlan::SubqueryAlias(SubqueryAlias {
                        input: Arc::new(input),
                        alias,
                        schema,
                    })
                }),
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
            LogicalPlan::Limit(Limit { input, fetch, skip }) => f(*input)?.update(|input| {
                LogicalPlan::Limit(Limit {
                    input: Box::new(input),
                    fetch,
                    skip,
                })
            }),
            LogicalPlan::Filter(Filter { expr, input }) => f(*input)?.update(|input| {
                LogicalPlan::Filter(Filter {
                    expr,
                    input: Box::new(input),
                })
            }),
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                on,
                filter,
                schema,
            }) => Transformed::yes(LogicalPlan::Join(Join {
                left: f(Arc::unwrap_or_clone(left)).data().map(Arc::new)?,
                right: f(Arc::unwrap_or_clone(right)).data().map(Arc::new)?,
                join_type,
                on,
                filter,
                schema,
            })),
            _ => Transformed::no(self),
        })
    }

    fn apply_children<'n, F>(&'n self, mut f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&'n LogicalPlan) -> Result<TreeNodeRecursion>,
    {
        for child in self.children().into_iter().flatten() {
            match f(child)? {
                TreeNodeRecursion::Continue => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

impl<'a, T: TransformNode + 'a> TreeNodeContainer<'a, T> for Vec<T> {
    fn apply<F>(&'a self, mut f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&'a T) -> Result<TreeNodeRecursion>,
    {
        for child in self {
            match child.apply(&mut f)? {
                TreeNodeRecursion::Continue => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }

        Ok(TreeNodeRecursion::Continue)
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
            LogicalPlan::CrossJoin(s) => write!(f, "{}", s),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmptyRelation {
    pub produce_one_row: bool,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
