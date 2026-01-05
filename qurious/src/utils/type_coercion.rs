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
        Operator::And | Operator::Or => Ok(BinaryTypes {
            lhs: lhs.clone(),
            rhs: rhs.clone(),
            ret: DataType::Boolean,
        }),

        // Comparisons: coerce operands to compatible types when possible
        Operator::Eq | Operator::NotEq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
            // Special-case implicit date/timestamp comparisons against string literals.
            // TPC-H and many SQL dialects allow: date_col >= '1993-07-01'
            // Our planner represents '1993-07-01' as Utf8, so we need to cast it to Date32.
            match (lhs, rhs) {
                (Date32, Utf8) | (Utf8, Date32) => Ok(BinaryTypes {
                    lhs: Date32,
                    rhs: Date32,
                    ret: DataType::Boolean,
                }),
                (Timestamp(_, _), Utf8) | (Utf8, Timestamp(_, _)) => Ok(BinaryTypes {
                    lhs: lhs.clone(),
                    rhs: rhs.clone(),
                    ret: DataType::Boolean,
                }),
                // Decimal comparisons against integral types: cast the integral side to the SAME decimal
                // precision/scale as the decimal side. Arrow doesn't allow comparing decimals with differing
                // precision/scale (e.g. Decimal128(15,2) < Decimal128(20,0)).
                (Decimal128(p, s), Int8 | Int16 | Int32 | Int64) => Ok(BinaryTypes {
                    lhs: DataType::Decimal128(*p, *s),
                    rhs: DataType::Decimal128(*p, *s),
                    ret: DataType::Boolean,
                }),
                (Int8 | Int16 | Int32 | Int64, Decimal128(p, s)) => Ok(BinaryTypes {
                    lhs: DataType::Decimal128(*p, *s),
                    rhs: DataType::Decimal128(*p, *s),
                    ret: DataType::Boolean,
                }),
                // Decimal comparisons against float types: cast float to the same decimal type as the decimal side.
                //
                // This is important for predicates like:
                //   decimal_col BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                // where the RHS is parsed as Float64 literals.
                (Decimal128(p, s), Float32 | Float64) => Ok(BinaryTypes {
                    lhs: DataType::Decimal128(*p, *s),
                    rhs: DataType::Decimal128(*p, *s),
                    ret: DataType::Boolean,
                }),
                (Float32 | Float64, Decimal128(p, s)) => Ok(BinaryTypes {
                    lhs: DataType::Decimal128(*p, *s),
                    rhs: DataType::Decimal128(*p, *s),
                    ret: DataType::Boolean,
                }),
                // Fallback: keep as-is (may error later if Arrow can't compare them)
                _ => Ok(BinaryTypes {
                    lhs: lhs.clone(),
                    rhs: rhs.clone(),
                    ret: DataType::Boolean,
                }),
            }
        }

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
