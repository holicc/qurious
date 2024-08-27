use arrow::datatypes::DataType;
use arrow::util::display::ArrayFormatter;
use arrow::util::display::DurationFormat;
use arrow::util::display::FormatOptions;
use arrow::{array, array::ArrayRef, record_batch::RecordBatch};
use async_trait::async_trait;
use itertools::Either;
use qurious::arrow_err;
use qurious::error::{Error, Result};
use qurious::execution::session::ExecuteSession;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use std::fs::read_dir;
use std::iter::once;
use std::path::PathBuf;

const TEST_SQL_DIR: &str = "./tests/sql/";

fn main() -> Result<()> {
    env_logger::init();

    log::info!("Running sqllogictests");

    read_test_files()?
        .into_par_iter()
        .map(make_test_session)
        .try_for_each(|session| {
            let session = session?;
            let mut runner = sqllogictest::Runner::new(|| async { Ok(&session) });

            runner
                .run_file(session.path.clone())
                .map_err(|e| Error::InternalError(e.to_string()))
        })
}

fn make_test_session(path: PathBuf) -> Result<TestSession> {
    ExecuteSession::new().map(|session| TestSession { session, path })
}

fn read_test_files() -> Result<Vec<PathBuf>> {
    let mut files = vec![];
    for entry in read_dir(TEST_SQL_DIR)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

struct TestSession {
    path: PathBuf,
    session: ExecuteSession,
}

#[async_trait]
impl AsyncDB for &TestSession {
    type Error = qurious::error::Error;

    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        log::info!("Running SQL: {}", sql);

        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };
        let batches = self.session.sql(sql)?;

        if batches.is_empty() {
            if is_query_sql {
                return Ok(DBOutput::Rows {
                    types: vec![],
                    rows: vec![],
                });
            } else {
                return Ok(DBOutput::StatementComplete(0));
            }
        }

        let types = vec![DefaultColumnType::Any; batches[0].num_columns()];
        let rows = convert_batches(batches)?;

        Ok(DBOutput::Rows { types, rows })
    }
}

fn convert_batches(batches: Vec<RecordBatch>) -> Result<Vec<Vec<String>>> {
    if batches.is_empty() {
        Ok(vec![])
    } else {
        let schema = batches[0].schema();
        let mut rows = vec![];
        for batch in batches {
            // Verify schema
            if !schema.contains(&batch.schema()) {
                return Err(Error::InternalError(format!(
                    "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                    &schema,
                    batch.schema()
                )));
            }

            let new_rows = convert_batch(batch)?.into_iter().flat_map(expand_row);

            rows.extend(new_rows);
        }
        Ok(rows)
    }
}

fn expand_row(mut row: Vec<String>) -> impl Iterator<Item = Vec<String>> {
    // check last cell
    if let Some(cell) = row.pop() {
        let lines: Vec<_> = cell.split('\n').collect();

        // no newlines in last cell
        if lines.len() < 2 {
            row.push(cell);
            return Either::Left(once(row));
        }

        // form new rows with each additional line
        let new_lines: Vec<_> = lines
            .into_iter()
            .enumerate()
            .map(|(idx, l)| {
                // replace any leading spaces with '-' as
                // `sqllogictest` ignores whitespace differences
                //
                // See https://github.com/apache/datafusion/issues/6328
                let content = l.trim_start();
                let new_prefix = "-".repeat(l.len() - content.len());
                // maintain for each line a number, so
                // reviewing explain result changes is easier
                let line_num = idx + 1;
                vec![format!("{line_num:02}){new_prefix}{content}")]
            })
            .collect();

        Either::Right(once(row).chain(new_lines))
    } else {
        Either::Left(once(row))
    }
}

fn convert_batch(batch: RecordBatch) -> Result<Vec<Vec<String>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|col| cell_to_string(col, row))
                .collect::<Result<Vec<String>>>()
        })
        .collect()
}

macro_rules! get_row_value {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        array.value($row)
    }};
}

fn cell_to_string(col: &ArrayRef, row: usize) -> Result<String> {
    let null_str = "NULL".to_owned();
    if !col.is_valid(row) {
        // represent any null value with the string "NULL"
        Ok(null_str)
    } else {
        match col.data_type() {
            DataType::Null => Ok(null_str),
            DataType::Boolean => Ok(bool_to_str(get_row_value!(array::BooleanArray, col, row))),
            DataType::Float32 => Ok(f32_to_str(get_row_value!(array::Float32Array, col, row))),
            DataType::Float64 => Ok(f64_to_str(get_row_value!(array::Float64Array, col, row))),
            DataType::LargeUtf8 => Ok(varchar_to_str(get_row_value!(array::LargeStringArray, col, row))),
            DataType::Utf8 => Ok(varchar_to_str(get_row_value!(array::StringArray, col, row))),
            DataType::Utf8View => Ok(varchar_to_str(get_row_value!(array::StringViewArray, col, row))),
            _ => {
                let f = ArrayFormatter::try_new(
                    col.as_ref(),
                    &FormatOptions::new().with_duration_format(DurationFormat::Pretty),
                );
                Ok(f.unwrap().value(row).to_string())
            }
        }
        .map_err(|e| arrow_err!(e))
    }
}

fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.trim_end_matches('\n').to_string()
    }
}

fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        value.to_string()
    }
}

fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        value.to_string()
    }
}
