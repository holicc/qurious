pub mod alias;
pub mod array;
pub mod batch;
pub mod expr;
pub mod type_coercion;

use std::path::Path;

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

pub fn get_file_type(file_path: &str) -> Option<&str> {
    let path = Path::new(file_path);
    path.extension().and_then(|ext| ext.to_str())
}
