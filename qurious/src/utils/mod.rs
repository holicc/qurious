pub mod array;
pub mod batch;
pub mod type_coercion;

use std::{path::Path, sync::Arc};

use crate::error::Result;
use arrow::datatypes::{Schema, SchemaBuilder};
use sqlparser::ast::Ident;

use crate::logical::plan::LogicalPlan;

pub fn version() -> String {
    format!("QURIOUS v{}", env!("CARGO_PKG_VERSION"))
}

pub fn format(plan: &LogicalPlan, ident: usize) -> String {
    let mut sb = String::new();

    (0..ident).for_each(|_| sb.push_str("  "));

    sb.push_str(&format!("{}\n", plan));

    if let Some(p) = plan.children() {
        for ele in p {
            sb.push_str(&format(ele, ident + 1));
        }
    }

    sb
}

pub fn normalize_ident(i: Ident) -> String {
    match i.quote_style {
        Some(_) => i.value,
        None => i.value.to_ascii_lowercase(),
    }
}

pub fn merge_schema(a: &Arc<Schema>, b: &Arc<Schema>) -> Result<Schema> {
    let mut builder = SchemaBuilder::from(a.as_ref());
    b.fields().iter().try_for_each(|f| builder.try_merge(f))?;
    Ok(builder.finish())
}

pub fn get_file_type(file_path: &str) -> Option<&str> {
    let path = Path::new(file_path);
    path.extension().and_then(|ext| ext.to_str())
}
