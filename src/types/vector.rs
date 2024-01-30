use std::{fmt::Debug, sync::Arc};

use super::{datatype::DataType, scalar::ScalarValue};

pub type ColumnarVectorRef = Arc<dyn ColumnVector>;

pub trait ColumnVector: Debug {
    fn data_type(&self) -> DataType;
    fn get_value(&self, index: usize) -> ScalarValue;
    fn size(&self) -> usize;
}
