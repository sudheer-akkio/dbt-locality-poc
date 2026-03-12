{%- macro locality_id_col() -%}
{%- if target.name == 'staging' -%}AKKIO_ID{%- else -%}LOCALITY_ID{%- endif -%}
{%- endmacro -%}

{%- macro locality_hh_id_col() -%}
{%- if target.name == 'staging' -%}AKKIO_HH_ID{%- else -%}LOCALITY_HH_ID{%- endif -%}
{%- endmacro -%}
