# Community 141: HasuraClient

**Members:** 8

## Nodes

- **field_selector_degrades_when_hooks_missing()** (`src_infra_hasura_rs_field_selector_degrades_when_hooks_missing`, Function, degree: 2)
- **HasuraClient** (`src_infra_hasura_rs_hasuraclient`, Struct, degree: 7)
- **.block_cursor_where()** (`src_infra_hasura_rs_hasuraclient_block_cursor_where`, Method, degree: 3)
- **.discover_pools()** (`src_infra_hasura_rs_hasuraclient_discover_pools`, Method, degree: 4)
- **.fetch_token_metas()** (`src_infra_hasura_rs_hasuraclient_fetch_token_metas`, Method, degree: 3)
- **.new()** (`src_infra_hasura_rs_hasuraclient_new`, Method, degree: 5)
- **.query()** (`src_infra_hasura_rs_hasuraclient_query`, Method, degree: 2)
- **.query_pool_meta_page()** (`src_infra_hasura_rs_hasuraclient_query_pool_meta_page`, Method, degree: 3)

## Relationships

- src_infra_hasura_rs_hasuraclient → src_infra_hasura_rs_hasuraclient_new (defines)
- src_infra_hasura_rs_hasuraclient → src_infra_hasura_rs_hasuraclient_query (defines)
- src_infra_hasura_rs_hasuraclient → src_infra_hasura_rs_hasuraclient_block_cursor_where (defines)
- src_infra_hasura_rs_hasuraclient → src_infra_hasura_rs_hasuraclient_query_pool_meta_page (defines)
- src_infra_hasura_rs_hasuraclient → src_infra_hasura_rs_hasuraclient_discover_pools (defines)
- src_infra_hasura_rs_hasuraclient → src_infra_hasura_rs_hasuraclient_fetch_token_metas (defines)
- src_infra_hasura_rs_hasuraclient_block_cursor_where → src_infra_hasura_rs_hasuraclient_new (calls)
- src_infra_hasura_rs_hasuraclient_discover_pools → src_infra_hasura_rs_hasuraclient_new (calls)
- src_infra_hasura_rs_hasuraclient_discover_pools → src_infra_hasura_rs_hasuraclient_block_cursor_where (calls)
- src_infra_hasura_rs_hasuraclient_discover_pools → src_infra_hasura_rs_hasuraclient_query_pool_meta_page (calls)
- src_infra_hasura_rs_hasuraclient_fetch_token_metas → src_infra_hasura_rs_hasuraclient_query (calls)
- src_infra_hasura_rs_hasuraclient_fetch_token_metas → src_infra_hasura_rs_hasuraclient_new (calls)
- src_infra_hasura_rs_field_selector_degrades_when_hooks_missing → src_infra_hasura_rs_hasuraclient_new (calls)

