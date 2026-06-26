# Community 117: returns_error_when_not_initialized()

**Members:** 9

## Nodes

- **.confirm()** (`src_services_execution_nonce_rs_noncemanager_confirm`, Method, degree: 3)
- **.new()** (`src_services_execution_nonce_rs_noncemanager_new`, Method, degree: 10)
- **.build()** (`src_services_execution_nonce_rs_noncemanagerbuilder_build`, Method, degree: 4)
- **NonceState** (`src_services_execution_nonce_rs_noncestate`, Struct, degree: 4)
- **.init()** (`src_services_execution_nonce_rs_noncestate_init`, Method, degree: 3)
- **.next_available()** (`src_services_execution_nonce_rs_noncestate_next_available`, Method, degree: 2)
- **.prune_stale()** (`src_services_execution_nonce_rs_noncestate_prune_stale`, Method, degree: 2)
- **reserves_and_confirms_sequential_nonces()** (`src_services_execution_nonce_rs_reserves_and_confirms_sequential_nonces`, Function, degree: 3)
- **returns_error_when_not_initialized()** (`src_services_execution_nonce_rs_returns_error_when_not_initialized`, Function, degree: 2)

## Relationships

- src_services_execution_nonce_rs_noncestate → src_services_execution_nonce_rs_noncestate_init (defines)
- src_services_execution_nonce_rs_noncestate → src_services_execution_nonce_rs_noncestate_next_available (defines)
- src_services_execution_nonce_rs_noncestate → src_services_execution_nonce_rs_noncestate_prune_stale (defines)
- src_services_execution_nonce_rs_noncestate_init → src_services_execution_nonce_rs_noncemanager_new (calls)
- src_services_execution_nonce_rs_noncemanagerbuilder_build → src_services_execution_nonce_rs_noncemanager_new (calls)
- src_services_execution_nonce_rs_noncemanagerbuilder_build → src_services_execution_nonce_rs_noncestate_init (calls)
- src_services_execution_nonce_rs_noncemanager_new → src_services_execution_nonce_rs_noncemanagerbuilder_build (calls)
- src_services_execution_nonce_rs_noncemanager_confirm → src_services_execution_nonce_rs_noncestate_prune_stale (calls)
- src_services_execution_nonce_rs_reserves_and_confirms_sequential_nonces → src_services_execution_nonce_rs_noncemanager_new (calls)
- src_services_execution_nonce_rs_reserves_and_confirms_sequential_nonces → src_services_execution_nonce_rs_noncemanager_confirm (calls)
- src_services_execution_nonce_rs_returns_error_when_not_initialized → src_services_execution_nonce_rs_noncemanager_new (calls)

