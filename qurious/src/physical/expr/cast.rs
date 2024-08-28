use std::{fmt::Display, sync::Arc};

use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use arrow::{
    array::{ArrayRef, RecordBatch},
    compute::{cast_with_options, CastOptions},
    datatypes::DataType::{self},
    util::display::{DurationFormat, FormatOptions},
};

pub const DEFAULT_FORMAT_OPTIONS: FormatOptions<'static> =
    FormatOptions::new().with_duration_format(DurationFormat::Pretty);

pub const DEFAULT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

#[derive(Debug)]
pub struct CastExpr {
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl CastExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

impl PhysicalExpr for CastExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        self.expr
            .evaluate(input)
            .and_then(|array| cast_with_options(&array, &self.data_type, &DEFAULT_CAST_OPTIONS).map_err(|e| e.into()))
    }
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.data_type)
    }
}
