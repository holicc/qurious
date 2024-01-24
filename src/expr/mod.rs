mod binary;
mod column;
mod literal;
mod operator;

pub use operator::Operator;

use crate::error::Result;
use crate::{logical_plan::plan::LogicalPlan, types::field::Field};

pub trait LogicalExpr {
    fn to_field(&self, plan: &impl LogicalPlan) -> Result<Field>;
}
