use arrow::datatypes::{DataType, Field, FieldRef};

use crate::error::{Error, Result};
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use std::convert::TryFrom;
use std::fmt::Display;
use std::sync::Arc;

use super::Column;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateOperator {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl AggregateOperator {
    pub fn infer_type(&self, expr_data_type: &DataType) -> Result<DataType> {
        match self {
            AggregateOperator::Count => Ok(DataType::Int64),
            AggregateOperator::Avg => Ok(DataType::Float64),
            _ => Ok(expr_data_type.clone()),
        }
    }
}

impl Display for AggregateOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateOperator::Sum => write!(f, "SUM"),
            AggregateOperator::Min => write!(f, "MIN"),
            AggregateOperator::Max => write!(f, "MAX"),
            AggregateOperator::Avg => write!(f, "AVG"),
            AggregateOperator::Count => write!(f, "COUNT"),
        }
    }
}

impl TryFrom<String> for AggregateOperator {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "sum" => Ok(AggregateOperator::Sum),
            "min" => Ok(AggregateOperator::Min),
            "max" => Ok(AggregateOperator::Max),
            "avg" => Ok(AggregateOperator::Avg),
            "count" => Ok(AggregateOperator::Count),
            _ => Err(Error::InternalError(format!(
                "{} is not a valid aggregate operator",
                value
            ))),
        }
    }
}

impl TryFrom<&str> for AggregateOperator {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::try_from(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    pub op: AggregateOperator,
    pub expr: Box<LogicalExpr>,
}

impl AggregateExpr {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        self.expr.field(plan).and_then(|field| {
            let col_name = if let LogicalExpr::Column(inner) = self.expr.as_ref() {
                &inner.quanlified_name()
            } else {
                field.name()
            };

            Ok(Arc::new(Field::new(
                format!("{}({})", self.op, col_name),
                self.op.infer_type(field.data_type())?,
                true,
            )))
        })
    }

    pub(crate) fn as_column(&self) -> Result<LogicalExpr> {
        self.expr.as_column().map(|inner_col| {
            LogicalExpr::Column(Column {
                name: format!("{}({})", self.op, inner_col),
                relation: None,
            })
        })
    }
}

impl Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.op, self.expr)
    }
}
