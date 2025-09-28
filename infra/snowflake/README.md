snowflake IaC (Terraform)

This folder provisions the Snowflake objects used by the case:
- database `WEATHER`
- schema `RAW`
- file format `FF_OPENWEATHER_JSONL`
- storage integration `GCS_INT` (points to your GCS path)
- stage `STG_OPENWEATHER`
- table `RAW_OPENWEATHER`

Requirements
- Terraform >= 1.5
- Snowflake provider (installed automatically)
- Snowflake credentials via environment variables

Env vars (example)
```bash
export SNOWFLAKE_ACCOUNT=IFNTRPR-AK19625
export SNOWFLAKE_USER=GOOSE
export SNOWFLAKE_PASSWORD='<SNOWFLAKE_PASSWORD>'
export SNOWFLAKE_ROLE=ACCOUNTADMIN
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# verify they are set in this shell
env | grep ^SNOWFLAKE_
```

- Run the exports in every shell before calling Terraform, or append them (minus secrets) to `~/.bashrc` and run `source ~/.bashrc` so they load automatically.
- Example helper to append (edit before running so you do not store plaintext secrets in history):
  ```bash
  cat >> ~/.bashrc <<'EOF'
  export SNOWFLAKE_ACCOUNT=IFNTRPR-AK19625
  export SNOWFLAKE_USER=GOOSE
  export SNOWFLAKE_ROLE=ACCOUNTADMIN
  export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  # export SNOWFLAKE_PASSWORD='<set this securely, e.g. via keyring>'
  EOF
  ```
  Then set `SNOWFLAKE_PASSWORD` in the shell right before Terraform runs, or use a password manager/`read -s` prompt instead of storing it in a file.

Usage
```bash
cd infra/snowflake
terraform init
## Option A: set TF_VAR_* (recommended)
export TF_VAR_snowflake_account=IFNTRPR-AK19625
export TF_VAR_snowflake_user=GOOSE
export TF_VAR_snowflake_password='<PROGRAMMATIC_PASSWORD>'
export TF_VAR_snowflake_role=ACCOUNTADMIN
export TF_VAR_snowflake_warehouse=COMPUTE_WH

terraform plan
terraform apply -auto-approve

## Option B: copy example tfvars
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your values, then
terraform plan
terraform apply -auto-approve

# Output shows STORAGE_GCP_SERVICE_ACCOUNT
# Grant GCS viewer on your bucket to that SA
gsutil iam ch serviceAccount:$(terraform output -raw storage_gcp_service_account):objectViewer gs://openweathermap_data
```

Notes
- The provider needs a role with privileges to create a storage integration (typically ACCOUNTADMIN).
- Adjust `gcs_url` in `main.tf` if your path differs.


