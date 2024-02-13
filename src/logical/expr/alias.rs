use std::fmt::Display;

#[derive(Debug,Clone)]
pub struct Alias {
    name: String,
}

impl Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Alias: {}", self.name)
    }
}
