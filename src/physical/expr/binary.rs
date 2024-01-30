use std::sync::Arc;

use crate::types::operator::Operator;

use super::PhysicalExpr;

pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(
        &self,
        input: &crate::types::batch::RecordBatch,
    ) -> crate::error::Result<crate::types::columnar::ColumnarValue> {
        todo!()
    }
}
