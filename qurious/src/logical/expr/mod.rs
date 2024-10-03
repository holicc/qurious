mod aggregate;
pub mod alias;
mod binary;
mod cast;
mod column;
mod function;
mod literal;
mod sort;

use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

pub use aggregate::{AggregateExpr, AggregateOperator};
pub use binary::*;
pub use cast::*;
pub use column::*;
pub use function::Function;
pub use literal::*;
pub use sort::*;

use crate::common::table_relation::TableRelation;
use crate::datatypes::scalar::ScalarValue;
use crate::error::{Error, Result};
use crate::logical::plan::LogicalPlan;
use arrow::datatypes::{DataType, Field, FieldRef};

use self::alias::Alias;
use crate::logical::plan::base_plan;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalExpr {
    Alias(Alias),
    Column(Column),
    Literal(ScalarValue),
    BinaryExpr(BinaryExpr),
    AggregateExpr(AggregateExpr),
    SortExpr(SortExpr),
    Cast(CastExpr),
    Wildcard,
    Function(Function),
    IsNull(Box<LogicalExpr>),
    IsNotNull(Box<LogicalExpr>),
    Negative(Box<LogicalExpr>),
}

macro_rules! impl_logical_expr_methods {
    ($($variant:ident),+ $(,)?) => {
        impl LogicalExpr {
            pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
                let base_plan = base_plan(plan);
                match self {
                    $(
                        LogicalExpr::$variant(e) => e.field(&base_plan),
                    )+
                    LogicalExpr::Literal(v) => Ok(Arc::new(v.to_field())),
                    LogicalExpr::Wildcard => Ok(Arc::new(Field::new("*", DataType::Null, true))),
                    _ => Err(Error::InternalError(format!(
                        "Cannot determine schema for expression: {:?}",
                        self
                    ))),
                }
            }
        }
    };
}

impl_logical_expr_methods! {
    Column,
    BinaryExpr,
    AggregateExpr,
    Alias,
    Cast,
    Function,
    IsNotNull,
    IsNull,
    Negative,
}

impl Display for LogicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalExpr::Negative(e) => write!(f, "- {}", e),
            LogicalExpr::Literal(v) => write!(f, "{}", v),
            LogicalExpr::Wildcard => write!(f, "*"),
            LogicalExpr::Alias(alias) => write!(f, "{} AS {}", alias.expr, alias.name),
            LogicalExpr::Column(column) => write!(f, "{column}"),
            LogicalExpr::BinaryExpr(binary_expr) => write!(f, "{binary_expr}",),
            LogicalExpr::AggregateExpr(aggregate_expr) => write!(f, "{aggregate_expr}",),
            LogicalExpr::SortExpr(sort_expr) => write!(f, "{sort_expr}",),
            LogicalExpr::Cast(cast_expr) => write!(f, "CAST({} AS {})", cast_expr.expr, cast_expr.data_type),
            LogicalExpr::Function(function) => write!(f, "{function}",),
            LogicalExpr::IsNull(logical_expr) => write!(f, "{} IS NULL", logical_expr),
            LogicalExpr::IsNotNull(logical_expr) => write!(f, "{} IS NOT NULL", logical_expr),
        }
    }
}

impl LogicalExpr {
    pub fn using_columns(&self) -> HashSet<Column> {
        let mut columns = HashSet::new();
        let mut stack = vec![self];

        while let Some(expr) = stack.pop() {
            match expr {
                LogicalExpr::Column(a) => {
                    columns.insert(a.clone());
                }
                LogicalExpr::Alias(a) => {
                    stack.push(&a.expr);
                }
                LogicalExpr::BinaryExpr(binary_op) => {
                    stack.push(&binary_op.left);
                    stack.push(&binary_op.right);
                }
                LogicalExpr::AggregateExpr(ag) => {
                    stack.push(&ag.expr);
                }
                _ => {}
            }
        }

        columns
    }

    pub fn cast_to(&self, data_type: &DataType) -> LogicalExpr {
        LogicalExpr::Cast(CastExpr {
            expr: Box::new(self.clone()),
            data_type: data_type.clone(),
        })
    }

    pub fn alias(&self, name: impl Into<String>) -> LogicalExpr {
        LogicalExpr::Alias(Alias {
            expr: Box::new(self.clone()),
            name: name.into(),
        })
    }

    pub fn as_column(&self) -> Result<LogicalExpr> {
        match self {
            LogicalExpr::Column(_) => Ok(self.clone()),
            LogicalExpr::Wildcard | LogicalExpr::BinaryExpr(_) | LogicalExpr::AggregateExpr(_) => Ok(
                LogicalExpr::Column(Column::new(format!("{}", self), None::<TableRelation>)),
            ),
            _ => Err(Error::InternalError(format!("Expect column, got {:?}", self))),
        }
    }

    pub fn column_refs(&self) -> HashSet<&Column> {
        todo!()
    }
}

pub(crate) fn get_expr_value(expr: LogicalExpr) -> Result<i64> {
    match expr {
        LogicalExpr::Literal(ScalarValue::Int64(Some(v))) => Ok(v),
        _ => Err(Error::InternalError(format!("Unexpected expression in"))),
    }
}
