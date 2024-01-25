mod aggregate;
mod filter;
mod projection;
mod scan;

use crate::types::schema::Schema;
use std::fmt::Display;

pub trait LogicalPlan: Display {
    fn schema(&self) -> &Schema;

    fn children(&self) -> Option<Vec<&dyn LogicalPlan>>;

    fn to_string(&self, ident: usize) -> String {
        let mut result = String::new();
        for _ in 0..ident {
            result.push_str("\t");
        }

        result.push_str(format!("{}\n", self).as_str());

        if let Some(children) = self.children() {
            for child in children {
                result.push_str(&child.to_string(ident + 1));
            }
        }

        result
    }
}
