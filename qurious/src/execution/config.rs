pub struct SessionConfig {
    pub default_catalog: String,
    pub default_schema: String,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            default_catalog: "qurious".to_string(),
            default_schema: "public".to_string(),
        }
    }
}
