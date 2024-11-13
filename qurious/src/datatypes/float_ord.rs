use std::cmp::Ordering;

#[derive(PartialEq, PartialOrd)]
pub struct F32Ord(pub f32);

#[derive(PartialEq, PartialOrd)]
pub struct F64Ord(pub f64);

impl Eq for F32Ord {}
impl Eq for F64Ord {}

impl Ord for F32Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or_else(|| {
            if self.0.is_nan() {
                if other.0.is_nan() {
                    Ordering::Equal
                } else {
                    Ordering::Greater // Place NaN values at the end
                }
            } else {
                Ordering::Less
            }
        })
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or_else(|| {
            if self.0.is_nan() {
                if other.0.is_nan() {
                    Ordering::Equal
                } else {
                    Ordering::Greater // Place NaN values at the end
                }
            } else {
                Ordering::Less
            }
        })
    }
}
