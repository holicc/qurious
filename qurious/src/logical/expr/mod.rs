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
use crate::common::transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion};
use crate::datatypes::scalar::ScalarValue;
use crate::error::{Error, Result};
use crate::internal_err;
use crate::logical::plan::LogicalPlan;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};

use self::alias::Alias;

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
                match self {
                    $(
                        LogicalExpr::$variant(e) => e.field(plan),
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
    pub fn rebase_expr(self, base_exprs: &[&LogicalExpr]) -> Result<Self> {
        self.transform(|nested_expr| {
            if base_exprs.contains(&&nested_expr) {
                return nested_expr.as_column().map(Transformed::yes);
            }
            Ok(Transformed::no(nested_expr))
        })
        .data()
    }

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

    pub fn cast_to(self, data_type: &DataType) -> LogicalExpr {
        LogicalExpr::Cast(CastExpr {
            expr: Box::new(self),
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
            LogicalExpr::AggregateExpr(agg) => agg.as_column(),
            LogicalExpr::Literal(_) | LogicalExpr::Wildcard | LogicalExpr::BinaryExpr(_) => Ok(LogicalExpr::Column(
                Column::new(format!("{}", self), None::<TableRelation>),
            )),
            _ => Err(Error::InternalError(format!("Expect column, got {:?}", self))),
        }
    }

    pub fn column_refs(&self) -> HashSet<&Column> {
        todo!()
    }

    pub fn data_type(&self, schema: &Arc<Schema>) -> Result<DataType> {
        match self {
            LogicalExpr::Alias(Alias { expr, .. }) => expr.data_type(schema),
            LogicalExpr::Column(column) => {
                let field = schema.field_with_name(&column.name)?;
                Ok(field.data_type().clone())
            }
            LogicalExpr::Literal(scalar_value) => Ok(scalar_value.data_type()),
            LogicalExpr::BinaryExpr(binary_expr) => binary_expr.get_result_type(schema),
            LogicalExpr::Cast(cast_expr) => Ok(cast_expr.data_type.clone()),
            LogicalExpr::Function(function) => Ok(function.func.return_type()),
            LogicalExpr::AggregateExpr(AggregateExpr { expr, .. })
            | LogicalExpr::SortExpr(SortExpr { expr, .. })
            | LogicalExpr::Negative(expr) => expr.data_type(schema),
            LogicalExpr::IsNull(_) | LogicalExpr::IsNotNull(_) => Ok(DataType::Boolean),
            LogicalExpr::Wildcard => internal_err!("Wildcard has no data type"),
        }
    }
}

impl TransformNode for LogicalExpr {
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(self, mut f: F) -> Result<Transformed<Self>> {
        Ok(match self {
            LogicalExpr::Alias(Alias { expr, name }) => f(*expr)?.update(|expr| {
                LogicalExpr::Alias(Alias {
                    expr: Box::new(expr),
                    name,
                })
            }),
            LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left = f(*left)?;
                let right = f(*right)?;
                let transformed = left.transformed || right.transformed;
                Transformed {
                    data: LogicalExpr::BinaryExpr(BinaryExpr {
                        left: left.update(Box::new).data,
                        op,
                        right: right.update(Box::new).data,
                    }),
                    transformed,
                }
            }
            LogicalExpr::AggregateExpr(AggregateExpr { op, expr }) => f(*expr)?.update(|expr| {
                LogicalExpr::AggregateExpr(AggregateExpr {
                    op,
                    expr: Box::new(expr),
                })
            }),
            LogicalExpr::SortExpr(SortExpr { expr, asc }) => f(*expr)?.update(|expr| {
                LogicalExpr::SortExpr(SortExpr {
                    expr: Box::new(expr),
                    asc,
                })
            }),
            LogicalExpr::Cast(CastExpr { expr, data_type }) => f(*expr)?.update(|expr| {
                LogicalExpr::Cast(CastExpr {
                    expr: Box::new(expr),
                    data_type,
                })
            }),
            LogicalExpr::Function(Function { func, args }) => {
                let args = args
                    .into_iter()
                    .map(|expr| f(expr).map(|expr| expr.data))
                    .collect::<Result<Vec<_>>>()?;
                Transformed::yes(LogicalExpr::Function(Function { func, args }))
            }
            LogicalExpr::IsNull(expr) => f(*expr)?.update(|expr| LogicalExpr::IsNull(Box::new(expr))),
            LogicalExpr::IsNotNull(expr) => f(*expr)?.update(|expr| LogicalExpr::IsNotNull(Box::new(expr))),
            LogicalExpr::Negative(expr) => f(*expr)?.update(|expr| LogicalExpr::Negative(Box::new(expr))),

            LogicalExpr::Wildcard | LogicalExpr::Column(_) | LogicalExpr::Literal(_) => Transformed::no(self),
        })
    }

    fn apply_children<'n, F>(&'n self, mut f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&'n Self) -> Result<TreeNodeRecursion>,
    {
        let children = match self {
            LogicalExpr::BinaryExpr(BinaryExpr { left, right, .. }) => vec![left.as_ref(), right.as_ref()],
            LogicalExpr::Function(function) => function.args.iter().map(|expr| expr).collect(),
            LogicalExpr::Negative(expr)
            | LogicalExpr::Cast(CastExpr { expr, .. })
            | LogicalExpr::AggregateExpr(AggregateExpr { expr, .. })
            | LogicalExpr::SortExpr(SortExpr { expr, .. })
            | LogicalExpr::IsNull(expr)
            | LogicalExpr::IsNotNull(expr)
            | LogicalExpr::Alias(Alias { expr, .. }) => vec![expr.as_ref()],
            LogicalExpr::Wildcard | LogicalExpr::Column(_) | LogicalExpr::Literal(_) => vec![],
        };

        for expr in children {
            match f(expr)? {
                TreeNodeRecursion::Continue => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

pub(crate) fn get_expr_value(expr: LogicalExpr) -> Result<i64> {
    match expr {
        LogicalExpr::Literal(ScalarValue::Int64(Some(v))) => Ok(v),
        _ => Err(Error::InternalError(format!("Unexpected expression in"))),
    }
}
