# Community 146: GasOracle

**Members:** 7

## Nodes

- **GasOracle** (`src_services_execution_gas_oracle_rs_gasoracle`, Struct, degree: 7)
- **.conservative_gas_price()** (`src_services_execution_gas_oracle_rs_gasoracle_conservative_gas_price`, Method, degree: 2)
- **.default()** (`src_services_execution_gas_oracle_rs_gasoracle_default`, Method, degree: 2)
- **.new()** (`src_services_execution_gas_oracle_rs_gasoracle_new`, Method, degree: 4)
- **.refresh_once()** (`src_services_execution_gas_oracle_rs_gasoracle_refresh_once`, Method, degree: 3)
- **.snapshot()** (`src_services_execution_gas_oracle_rs_gasoracle_snapshot`, Method, degree: 2)
- **.start_background()** (`src_services_execution_gas_oracle_rs_gasoracle_start_background`, Method, degree: 3)

## Relationships

- src_services_execution_gas_oracle_rs_gasoracle → src_services_execution_gas_oracle_rs_gasoracle_default (defines)
- src_services_execution_gas_oracle_rs_gasoracle → src_services_execution_gas_oracle_rs_gasoracle_new (defines)
- src_services_execution_gas_oracle_rs_gasoracle → src_services_execution_gas_oracle_rs_gasoracle_snapshot (defines)
- src_services_execution_gas_oracle_rs_gasoracle → src_services_execution_gas_oracle_rs_gasoracle_conservative_gas_price (defines)
- src_services_execution_gas_oracle_rs_gasoracle → src_services_execution_gas_oracle_rs_gasoracle_refresh_once (defines)
- src_services_execution_gas_oracle_rs_gasoracle → src_services_execution_gas_oracle_rs_gasoracle_start_background (defines)
- src_services_execution_gas_oracle_rs_gasoracle_default → src_services_execution_gas_oracle_rs_gasoracle_new (calls)
- src_services_execution_gas_oracle_rs_gasoracle_conservative_gas_price → src_services_execution_gas_oracle_rs_gasoracle_snapshot (calls)
- src_services_execution_gas_oracle_rs_gasoracle_refresh_once → src_services_execution_gas_oracle_rs_gasoracle_new (calls)
- src_services_execution_gas_oracle_rs_gasoracle_start_background → src_services_execution_gas_oracle_rs_gasoracle_new (calls)
- src_services_execution_gas_oracle_rs_gasoracle_start_background → src_services_execution_gas_oracle_rs_gasoracle_refresh_once (calls)

