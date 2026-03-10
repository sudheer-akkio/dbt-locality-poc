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

4. Configure environment variables

    Copy `.env.example` to `.env` and fill in the values for your target environment:
    ```bash
    cp .env.example .env
    # Edit .env with your values, then:
    source .env
    ```

5. Configure profile in `~/.dbt/profiles.yml`

    The project uses a single `locality` profile with two targets:
    ```yaml
    locality:
      target: staging  # default target
      outputs:
        staging:
          type: databricks
          host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
          http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/<staging-warehouse-id>') }}"
          token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
          catalog: akkio
          schema: locality_poc
          threads: 4

        prod:
          type: databricks
          host: "{{ env_var('DBT_DATABRICKS_HOST', '<prod-host>') }}"
          http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/<prod-warehouse-id>') }}"
          catalog: locality_dev
          schema: akkio
          auth_type: oauth
          client_id: "{{ env_var('DBT_DATABRICKS_CLIENT_ID') }}"
          client_secret: "{{ env_var('DBT_DATABRICKS_CLIENT_SECRET') }}"
          threads: 8
    ```

### Environment Configuration

| Setting               | Staging                            | Prod                                                                |
| --------------------- | ---------------------------------- | ------------------------------------------------------------------- |
| Output catalog.schema | `akkio.locality_poc`               | `locality_dev.akkio`                                                |
| Source catalog        | `locality-poc-share` (Delta Share) | Set via `DBT_SOURCE_CATALOG` env var                                |
| Auth method           | Token (`DBT_DATABRICKS_TOKEN`)     | OAuth (`DBT_DATABRICKS_CLIENT_ID` / `DBT_DATABRICKS_CLIENT_SECRET`) |

**Environment Variables:**

| Variable                       | Required For | Description                                                                     |
| ------------------------------ | ------------ | ------------------------------------------------------------------------------- |
| `DBT_DATABRICKS_HOST`          | Both         | Databricks workspace host                                                       |
| `DBT_DATABRICKS_HTTP_PATH`     | Optional     | SQL warehouse path (has per-target defaults)                                    |
| `DBT_DATABRICKS_TOKEN`         | Staging      | Personal access token                                                           |
| `DBT_DATABRICKS_CLIENT_ID`     | Prod         | OAuth service principal client ID                                               |
| `DBT_DATABRICKS_CLIENT_SECRET` | Prod         | OAuth service principal client secret                                           |
| `DBT_SOURCE_CATALOG`           | Prod         | Catalog containing source tables (defaults to `locality-poc-share` for staging) |

### Running the Pipeline

```bash
# Run against staging (default target)
dbt run

# Run against prod
dbt run --target prod

# Run specific model
dbt run --select "specify_model"

# Full refresh for incremental models
dbt run --full-refresh --select "specify_model_1" "specify_model_2"

# Build and test together
dbt build --select locality
```

### Running in Databricks Jobs

Configure the dbt task in your Databricks Job with:
1. **Environment variables**: Set `DBT_SOURCE_CATALOG` and the auth variables for your target environment
2. **dbt command**: Add `--target staging` or `--target prod` to the dbt CLI arguments
3. **Git source**: Point to `main` branch (no more branch switching needed)

## Resources

### dbt Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- [dbt_utils package documentation](https://github.com/dbt-labs/dbt-utils)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions
- Find [dbt events](https://events.getdbt.com) near you

### Project Resources
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)