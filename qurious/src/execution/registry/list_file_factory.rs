#[derive(Debug)]
pub struct DefaultTableSourceFactory;

impl TableSourceFactory for DefaultTableSourceFactory {
    fn create(&self, name: &str) -> Result<Arc<dyn DataSource>> {
        let url = file::parse_path(name)
            .map_err(|e| Error::PlanError(format!("No table named '{}' found, cause: {}", name, e.to_string())))?;

        if url.scheme() != "file" {
            return Err(Error::InternalError(format!("Unsupported table source: {}", name)));
        }

        let path = url.path().to_string();
        let ext = path.split('.').last().unwrap_or_default();

        match ext {
            "csv" => file::csv::read_csv(path, CsvReadOptions::default()),
            "json" => file::json::read_json(path, JsonReadOptions::default()),
            "parquet" => file::parquet::read_parquet(path),
            _ => return Err(Error::InternalError(format!("Unsupported file format: {}", name))),
        }
    }
}