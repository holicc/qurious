use crate::types::field::Field;

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}


impl Schema{

    pub fn project(&self,indices:Vec<usize>)->Schema{
        todo!()
    }

    pub fn select(&self,names:Vec<String>)->Schema{
        todo!()
    }
}