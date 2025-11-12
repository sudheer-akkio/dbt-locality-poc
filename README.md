# DBT Pipeline for Locality POC

## Quick Start

### Setup

1. Create and activate virtual environment
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. Install dependencies
    ```bash
    pip install dbt-core dbt-databricks
    ```

3. Install dbt packages
    ```bash
    dbt deps
    ```

4. Configure profile in `~/.dbt/profiles.yml`
    ```yaml
    locality_poc_databricks:
        outputs:
            dev:
                type: databricks
                host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
                http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
                token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
                catalog: akkio
                schema: locality_poc
                threads: 4
        target: dev 
    ```

### Running the Pipeline

```bash
# Run all models
dbt run

# Run specific model
dbt run --select "specify_model"

# For models that have set materialized='incremental', if you want to do a full refresh on data load:
dbt run --full-refresh --select "specify_model_1" "specify_model_2"

# Build and test together
dbt build --select locality
```
## Resources

### dbt Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- [dbt_utils package documentation](https://github.com/dbt-labs/dbt-utils)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions
- Find [dbt events](https://events.getdbt.com) near you

### Project Resources
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)