pub mod array;

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
