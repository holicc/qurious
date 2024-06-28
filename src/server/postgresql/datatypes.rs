use std::sync::Arc;

use arrow::{array::*, datatypes::*};
use chrono::{NaiveDate, NaiveDateTime};
use pgwire::{
    api::{
        results::{DataRowEncoder, FieldInfo, QueryResponse, Response},
        Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::data::DataRow,
};
use tokio_stream::{self as stream, StreamExt};

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> bool {
    arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(idx)
}

fn get_bool_list_value(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<bool>> {
    let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
    list_arr
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> $pt {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .value(idx)
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
get_primitive_value!(get_u8_value, UInt8Type, u8);
get_primitive_value!(get_u16_value, UInt16Type, u16);
get_primitive_value!(get_u32_value, UInt32Type, u32);
get_primitive_value!(get_u64_value, UInt64Type, u64);
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);

macro_rules! get_primitive_list_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<$pt>> {
            let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            list_arr
                .as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .collect()
        }
    };

    ($name:ident, $t:ty, $pt:ty, $f:expr) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<$pt>> {
            let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            list_arr
                .as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .map(|val| val.map($f))
                .collect()
        }
    };
}

get_primitive_list_value!(get_i8_list_value, Int8Type, i8);
get_primitive_list_value!(get_i16_list_value, Int16Type, i16);
get_primitive_list_value!(get_i32_list_value, Int32Type, i32);
get_primitive_list_value!(get_i64_list_value, Int64Type, i64);
get_primitive_list_value!(get_u8_list_value, UInt8Type, i8, |val: u8| { val as i8 });
get_primitive_list_value!(get_u16_list_value, UInt16Type, i16, |val: u16| { val as i16 });
get_primitive_list_value!(get_u32_list_value, UInt32Type, u32);
get_primitive_list_value!(get_u64_list_value, UInt64Type, i64, |val: u64| { val as i64 });
get_primitive_list_value!(get_f32_list_value, Float32Type, f32);
get_primitive_list_value!(get_f64_list_value, Float64Type, f64);

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> &str {
    arr.as_any().downcast_ref::<StringArray>().unwrap().value(idx)
}

fn get_large_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> &str {
    arr.as_any().downcast_ref::<LargeStringArray>().unwrap().value(idx)
}

fn get_date32_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    arr.as_any().downcast_ref::<Date32Array>().unwrap().value_as_date(idx)
}

fn get_date64_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    arr.as_any().downcast_ref::<Date64Array>().unwrap().value_as_date(idx)
}

fn get_time32_second_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_time32_millisecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_time64_microsecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}
fn get_time64_nanosecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_timestamp_second_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_timestamp_millisecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_timestamp_microsecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_timestamp_nanosecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    arr.as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_utf8_list_value(arr: &Arc<dyn Array>, idx: usize) -> Vec<Option<String>> {
    let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
    list_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|opt| opt.map(|val| val.to_owned()))
        .collect()
}

pub(crate) fn into_pg_reponse<'a>(batch: Vec<RecordBatch>) -> PgWireResult<Response<'a>> {
    if batch.is_empty() {
        return Ok(Response::EmptyQuery);
    }
    let schema = batch[0].schema();
    let fields = schema
        .fields
        .iter()
        .map(|f| into_pg_type(f))
        .collect::<PgWireResult<Vec<_>>>()
        .map(Arc::new)?;

    let row_stream = batch
        .into_iter()
        .map(|rb| {
            let rows = rb.num_rows();
            let cols = rb.num_columns();
            let fs = fields.clone();

            let row_stream = (0..rows).map(move |row| {
                let mut encoder = DataRowEncoder::new(fs.clone());
                for col in 0..cols {
                    let array = rb.column(col);
                    if array.is_null(row) {
                        encoder.encode_field(&None::<i8>).unwrap();
                    } else {
                        encode_value(&mut encoder, array, row).unwrap();
                    }
                }
                encoder.finish()
            });

            row_stream
        })
        .flatten()
        .collect::<Vec<PgWireResult<DataRow>>>();

    let rsp = QueryResponse::new(fields, stream::iter(row_stream));

    Ok(Response::Query(rsp))
}

fn encode_value(encoder: &mut DataRowEncoder, arr: &Arc<dyn Array>, idx: usize) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Null => encoder.encode_field(&None::<i8>)?,
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx))?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx))?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx))?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx))?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx))?,
        DataType::UInt8 => encoder.encode_field(&(get_u8_value(arr, idx) as i8))?,
        DataType::UInt16 => encoder.encode_field(&(get_u16_value(arr, idx) as i16))?,
        DataType::UInt32 => encoder.encode_field(&get_u32_value(arr, idx))?,
        DataType::UInt64 => encoder.encode_field(&(get_u64_value(arr, idx) as i64))?,
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx))?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx))?,
        DataType::Utf8 => encoder.encode_field(&get_utf8_value(arr, idx))?,
        DataType::LargeUtf8 => encoder.encode_field(&get_large_utf8_value(arr, idx))?,
        DataType::Date32 => encoder.encode_field(&get_date32_value(arr, idx))?,
        DataType::Date64 => encoder.encode_field(&get_date64_value(arr, idx))?,
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => encoder.encode_field(&get_time32_second_value(arr, idx))?,
            TimeUnit::Millisecond => encoder.encode_field(&get_time32_millisecond_value(arr, idx))?,
            _ => {}
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => encoder.encode_field(&get_time64_microsecond_value(arr, idx))?,
            TimeUnit::Nanosecond => encoder.encode_field(&get_time64_nanosecond_value(arr, idx))?,
            _ => {}
        },
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => encoder.encode_field(&get_timestamp_second_value(arr, idx))?,
            TimeUnit::Millisecond => encoder.encode_field(&get_timestamp_millisecond_value(arr, idx))?,
            TimeUnit::Microsecond => encoder.encode_field(&get_timestamp_microsecond_value(arr, idx))?,
            TimeUnit::Nanosecond => encoder.encode_field(&get_timestamp_nanosecond_value(arr, idx))?,
        },
        DataType::List(field) => match field.data_type() {
            DataType::Null => encoder.encode_field(&None::<i8>)?,
            DataType::Boolean => encoder.encode_field(&get_bool_list_value(arr, idx))?,
            DataType::Int8 => encoder.encode_field(&get_i8_list_value(arr, idx))?,
            DataType::Int16 => encoder.encode_field(&get_i16_list_value(arr, idx))?,
            DataType::Int32 => encoder.encode_field(&get_i32_list_value(arr, idx))?,
            DataType::Int64 => encoder.encode_field(&get_i64_list_value(arr, idx))?,
            DataType::UInt8 => encoder.encode_field(&get_u8_list_value(arr, idx))?,
            DataType::UInt16 => encoder.encode_field(&get_u16_list_value(arr, idx))?,
            DataType::UInt32 => encoder.encode_field(&get_u32_list_value(arr, idx))?,
            DataType::UInt64 => encoder.encode_field(&get_u64_list_value(arr, idx))?,
            DataType::Float32 => encoder.encode_field(&get_f32_list_value(arr, idx))?,
            DataType::Float64 => encoder.encode_field(&get_f64_list_value(arr, idx))?,
            DataType::Utf8 => encoder.encode_field(&get_utf8_list_value(arr, idx))?,
            // TODO: more types
            list_type => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Unsupported List Datatype {} and array {:?}", list_type, &arr),
                ))))
            }
        },

        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX001".to_owned(),
                format!("Unsupported Datatype {} and array {:?}", arr.data_type(), &arr),
            ))))
        }
    }
    Ok(())
}

pub(crate) fn into_pg_type(f: &Field) -> PgWireResult<FieldInfo> {
    let pg_type = match f.data_type() {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::UInt8 => Type::CHAR,
        DataType::Int16 | DataType::UInt16 => Type::INT2,
        DataType::Int32 | DataType::UInt32 => Type::INT4,
        DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Binary => Type::BYTEA,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        DataType::LargeUtf8 => Type::TEXT,
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Int8 | DataType::UInt8 => Type::CHAR_ARRAY,
            DataType::Int16 | DataType::UInt16 => Type::INT2_ARRAY,
            DataType::Int32 | DataType::UInt32 => Type::INT4_ARRAY,
            DataType::Int64 | DataType::UInt64 => Type::INT8_ARRAY,
            DataType::Timestamp(_, _) => Type::TIMESTAMP_ARRAY,
            DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
            DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
            DataType::Binary => Type::BYTEA_ARRAY,
            DataType::Float32 => Type::FLOAT4_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::Utf8 => Type::VARCHAR_ARRAY,
            DataType::LargeUtf8 => Type::TEXT_ARRAY,
            list_type => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Unsupported List Datatype {list_type}"),
                ))));
            }
        },
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX002".to_owned(),
                format!("Unsupported Datatype {}", f.data_type()),
            ))));
        }
    };

    Ok(FieldInfo::new(
        f.name().to_owned(),
        None,
        None,
        pg_type,
        pgwire::api::results::FieldFormat::Text,
    ))
}
