use arrow2::{datatypes::DataType, scalar::Scalar};

use crate::columner::ColumnVector;

pub struct LiteralColumnVector<S: Scalar> {
    value: S,
    size: usize,
}

impl<S: Scalar> ColumnVector for LiteralColumnVector<S> {
    
}
