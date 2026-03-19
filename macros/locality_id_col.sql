{%- macro get_env() -%}
{#- Resolution order:
    1. --vars '{"env":"prod"}'  (Databricks job params / explicit override)
    2. target.name              (local: dbt run --target prod)
    Default: staging -#}
{%- if var('env', 'staging') != 'staging' -%}
  {{- var('env') -}}
{%- elif target.name == 'prod' -%}
  {{- 'prod' -}}
{%- else -%}
  {{- 'staging' -}}
{%- endif -%}
{%- endmacro -%}

{%- macro locality_id_col() -%}
{%- if get_env() == 'staging' -%}AKKIO_ID{%- else -%}LOCALITY_ID{%- endif -%}
{%- endmacro -%}

{%- macro locality_hh_id_col() -%}
{%- if get_env() == 'staging' -%}AKKIO_HH_ID{%- else -%}LOCALITY_HH_ID{%- endif -%}
{%- endmacro -%}
