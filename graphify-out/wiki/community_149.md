# Community 149: pending_nonce()

**Members:** 7

## Nodes

- **NonceManager** (`src_services_execution_nonce_rs_noncemanager`, Struct, degree: 12)
- **.address()** (`src_services_execution_nonce_rs_noncemanager_address`, Method, degree: 1)
- **.in_flight_count()** (`src_services_execution_nonce_rs_noncemanager_in_flight_count`, Method, degree: 1)
- **.initialize()** (`src_services_execution_nonce_rs_noncemanager_initialize`, Method, degree: 2)
- **.resync()** (`src_services_execution_nonce_rs_noncemanager_resync`, Method, degree: 2)
- **.stale_count()** (`src_services_execution_nonce_rs_noncemanager_stale_count`, Method, degree: 1)
- **pending_nonce()** (`src_services_execution_nonce_rs_pending_nonce`, Function, degree: 3)

## Relationships

- src_services_execution_nonce_rs_noncemanager → src_services_execution_nonce_rs_noncemanager_address (defines)
- src_services_execution_nonce_rs_noncemanager → src_services_execution_nonce_rs_noncemanager_initialize (defines)
- src_services_execution_nonce_rs_noncemanager → src_services_execution_nonce_rs_noncemanager_stale_count (defines)
- src_services_execution_nonce_rs_noncemanager → src_services_execution_nonce_rs_noncemanager_in_flight_count (defines)
- src_services_execution_nonce_rs_noncemanager → src_services_execution_nonce_rs_noncemanager_resync (defines)
- src_services_execution_nonce_rs_noncemanager_initialize → src_services_execution_nonce_rs_pending_nonce (calls)
- src_services_execution_nonce_rs_noncemanager_resync → src_services_execution_nonce_rs_pending_nonce (calls)

