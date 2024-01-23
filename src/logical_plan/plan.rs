use std::fmt::Display;

use crate::types::schema::Schema;

pub trait LogicalPlan: Display {
    fn schema(&self) -> &Schema;

    fn children(&self) -> Vec<&dyn LogicalPlan>;

    fn to_string(&self, ident: usize) -> String {
        let mut result = String::new();
        for _ in 0..ident {
            result.push_str("\t");
        }

        result.push_str(format!("{}\n", self).as_str());

        for child in self.children() {
            result.push_str(&child.to_string(ident + 1));
        }
        result
    }
}
