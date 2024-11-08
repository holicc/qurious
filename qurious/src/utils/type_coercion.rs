use crate::datatypes::operator::Operator;
use crate::internal_err;
use crate::{
    arrow_err,
    error::{Error, Result},
};
use arrow::{
    array::new_empty_array,
    compute::kernels::numeric::{add_wrapping, div, mul_wrapping, rem, sub_wrapping},
    datatypes::DataType::{self, *},
};

pub fn get_input_types(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<(DataType, DataType)> {
    coercion_types(lhs, op, rhs).map(|x| (x.lhs, x.rhs))
}

pub fn get_result_type(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<DataType> {
    coercion_types(lhs, op, rhs).map(|x| x.ret)
}

struct BinaryTypes {
    lhs: DataType,
    rhs: DataType,
    ret: DataType,
}

impl BinaryTypes {
    fn uniform(dt: DataType) -> Self {
        Self {
            lhs: dt.clone(),
            rhs: dt.clone(),
            ret: dt,
        }
    }
}

fn coercion_types(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<BinaryTypes> {
    match op {
        Operator::And
        | Operator::Or
        | Operator::Eq
        | Operator::NotEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::Lt
        | Operator::LtEq => Ok(BinaryTypes {
            lhs: lhs.clone(),
            rhs: rhs.clone(),
            ret: DataType::Boolean,
        }),

        Operator::Add | Operator::Sub | Operator::Mul | Operator::Div | Operator::Mod => try_coerce(lhs, op, rhs)
            .or(decimal_coercion(lhs, op, rhs))
            .or(numeric_coercion(lhs, rhs)),
    }
}

fn try_coerce(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<BinaryTypes> {
    let l = new_empty_array(lhs);
    let r = new_empty_array(rhs);

    let result = match op {
        Operator::Add => add_wrapping(&l, &r),
        Operator::Sub => sub_wrapping(&l, &r),
        Operator::Mul => mul_wrapping(&l, &r),
        Operator::Div => div(&l, &r),
        Operator::Mod => rem(&l, &r),
        _ => unreachable!(),
    };

    result.map_err(|e| arrow_err!(e)).map(|x| BinaryTypes {
        lhs: lhs.clone(),
        rhs: rhs.clone(),
        ret: x.data_type().clone(),
    })
}

fn numeric_coercion(lhs: &DataType, rhs: &DataType) -> Result<BinaryTypes> {
    match (lhs, rhs) {
        (Float64, _) | (_, Float64) => Ok(BinaryTypes::uniform(Float64)),
        (_, Float32) | (Float32, _) => Ok(BinaryTypes::uniform(Float32)),
        (Int64, _) | (_, Int64) => Ok(BinaryTypes::uniform(Int64)),
        (UInt16, _) | (_, UInt16) => Ok(BinaryTypes::uniform(UInt16)),
        (UInt8, _) | (_, UInt8) => Ok(BinaryTypes::uniform(UInt8)),
        _ => internal_err!("can not coerce type: {lhs} and {rhs} for numeric operation"),
    }
}

fn decimal_coercion(lhs: &DataType, op: &Operator, rhs: &DataType) -> Result<BinaryTypes> {
    let (lhs, rhs) = match (lhs, rhs) {
        (Decimal128(_, _), Int8 | Int16 | Int32 | Int64) => Ok((lhs.clone(), coerce_numeric_type_to_decimal(rhs)?)),
        (Int8 | Int16 | Int32 | Int64, Decimal128(_, _)) => Ok((coerce_numeric_type_to_decimal(lhs)?, rhs.clone())),

        _ => internal_err!("decimal coercion not supported for {:?} and {:?}", lhs, rhs),
    }?;

    try_coerce(&lhs, op, &rhs)
}

fn coerce_numeric_type_to_decimal(dt: &DataType) -> Result<DataType> {
    match dt {
        Int8 => Ok(Decimal128(3, 0)),
        Int16 => Ok(Decimal128(5, 0)),
        Int32 => Ok(Decimal128(10, 0)),
        Int64 => Ok(Decimal128(20, 0)),
        _ => internal_err!("can not coerce type: {dt} to decimal"),
    }
}
