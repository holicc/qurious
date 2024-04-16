pub struct NestLoopJoin {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
}

impl NestLoopJoin {
    pub fn try_new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        join_type: JoinType,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        todo!()
    }
}

impl PhysicalPlan for NestLoopJoin {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let join_schema = join_schema(&left_schema, &right_schema, &self.join_type);

        todo!()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}