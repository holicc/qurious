use crate::types::field::Field;

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn project(&self, _indices: Vec<usize>) -> Schema {
        todo!()
    }

    pub fn select(&self, names: Vec<String>) -> &Schema {
        todo!()
    }
}
