# Community 121: try_from_env()

**Members:** 8

## Nodes

- **HyperSyncService** (`src_infra_hypersync_rs_hypersyncservice`, Struct, degree: 7)
- **.chain_id()** (`src_infra_hypersync_rs_hypersyncservice_chain_id`, Method, degree: 2)
- **.from_config()** (`src_infra_hypersync_rs_hypersyncservice_from_config`, Method, degree: 3)
- **.get_height()** (`src_infra_hypersync_rs_hypersyncservice_get_height`, Method, degree: 2)
- **.get_transaction_receipt()** (`src_infra_hypersync_rs_hypersyncservice_get_transaction_receipt`, Method, degree: 2)
- **.inner()** (`src_infra_hypersync_rs_hypersyncservice_inner`, Method, degree: 1)
- **.stream_height()** (`src_infra_hypersync_rs_hypersyncservice_stream_height`, Method, degree: 1)
- **try_from_env()** (`src_infra_hypersync_rs_try_from_env`, Function, degree: 2)

## Relationships

- src_infra_hypersync_rs_hypersyncservice → src_infra_hypersync_rs_hypersyncservice_from_config (defines)
- src_infra_hypersync_rs_hypersyncservice → src_infra_hypersync_rs_hypersyncservice_chain_id (defines)
- src_infra_hypersync_rs_hypersyncservice → src_infra_hypersync_rs_hypersyncservice_get_height (defines)
- src_infra_hypersync_rs_hypersyncservice → src_infra_hypersync_rs_hypersyncservice_stream_height (defines)
- src_infra_hypersync_rs_hypersyncservice → src_infra_hypersync_rs_hypersyncservice_inner (defines)
- src_infra_hypersync_rs_hypersyncservice → src_infra_hypersync_rs_hypersyncservice_get_transaction_receipt (defines)
- src_infra_hypersync_rs_hypersyncservice_from_config → src_infra_hypersync_rs_hypersyncservice_chain_id (calls)
- src_infra_hypersync_rs_hypersyncservice_get_transaction_receipt → src_infra_hypersync_rs_hypersyncservice_get_height (calls)
- src_infra_hypersync_rs_try_from_env → src_infra_hypersync_rs_hypersyncservice_from_config (calls)

