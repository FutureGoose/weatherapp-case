# Minimal Snowflake IaC to reproduce the objects created manually

locals {
  db_name     = "WEATHER"
  schema_name = "RAW"
  curated_schema_name = "CURATED"
  stage_name  = "STG_OPENWEATHER"
  ff_name     = "FF_OPENWEATHER_JSONL"
  integ_name  = "GCS_INT"
  gcs_url     = "gcs://openweathermap_data/openweather/"
}
#        type                 label
resource "snowflake_database" "weather" {
  name = local.db_name
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.weather.name
  name     = local.schema_name

  is_transient                = false
  with_managed_access         = false
}

# curated schema for modeled tables/views
resource "snowflake_schema" "curated" {
  database = snowflake_database.weather.name
  name     = local.curated_schema_name

  is_transient        = false
  with_managed_access = false
}

resource "snowflake_file_format" "jsonl" {
  database = snowflake_database.weather.name
  schema   = snowflake_schema.raw.name
  name     = local.ff_name
  format_type = "JSON"

  skip_byte_order_mark = true
}

# storage integration (requires ACCOUNTADMIN)
resource "snowflake_storage_integration" "gcs" {
  name                      = local.integ_name
  storage_provider          = "GCS"
  enabled                   = true
  storage_allowed_locations = [local.gcs_url]
}

# external stage pointing to GCS bucket path
resource "snowflake_stage" "stg" {
  database = snowflake_database.weather.name
  schema   = snowflake_schema.raw.name
  name     = local.stage_name

  url                   = local.gcs_url
  storage_integration   = snowflake_storage_integration.gcs.name
  file_format           = "FORMAT_NAME = ${snowflake_database.weather.name}.${snowflake_schema.raw.name}.ff_openweather_jsonl"
}

# raw landing table
resource "snowflake_table" "raw_openweather" {
  database = snowflake_database.weather.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_OPENWEATHER"

  data_retention_time_in_days = 1

  column {
    name = "DATA"
    type = "VARIANT"
  }
}

output "storage_gcp_service_account" {
  # grant this SA objectViewer on your GCS bucket
  value = snowflake_storage_integration.gcs.storage_gcp_service_account
}


