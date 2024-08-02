use std::{fmt::Display, sync::Arc};

use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use arrow::{
    array::{new_null_array, Array, ArrayRef, GenericStringBuilder, OffsetSizeTrait, RecordBatch},
    datatypes::DataType::{self, *},
    util::display::{ArrayFormatter, FormatOptions},
};

#[derive(Debug)]
pub struct CastExpr {
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl CastExpr{
    pub fn new(expr: Arc<dyn PhysicalExpr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

impl PhysicalExpr for CastExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let array = self.expr.evaluate(input)?;

        let from_type = array.data_type();
        let to_type = &self.data_type;
        match (from_type, to_type) {
            (Null, _) => Ok(new_null_array(to_type, array.len())),
            (_, Utf8) => to_str_array::<i32>(&array),
            (_, LargeUtf8) => to_str_array::<i64>(&array),
            _ => todo!(),
        }
    }
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.data_type)
    }
}

pub(crate) fn to_str_array<O: OffsetSizeTrait>(array: &dyn Array) -> Result<ArrayRef> {
    let mut builder = GenericStringBuilder::<O>::new();
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
    let nulls = array.nulls();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                formatter.value(i).write(&mut builder)?;
                // tell the builder the row is finished
                builder.append_value("");
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
