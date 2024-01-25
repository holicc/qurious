mod aggregate;
mod binary;
mod column;
mod literal;
mod operator;

pub use aggregate::{AggregateExpr, AggregateOperator};
pub use operator::Operator;

use crate::error::Result;
use crate::{logical_plan::LogicalPlan, types::field::Field};
use std::fmt::Display;

pub trait LogicalExpr: Display {
    fn to_field(&self, plan: &dyn LogicalPlan) -> Result<Field>;
}
