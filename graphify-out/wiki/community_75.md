# Community 75: RpcPool

**Members:** 12

## Nodes

- **RpcPool** (`src_infra_rpc_rs_rpcpool`, Struct, degree: 12)
- **.connect_http()** (`src_infra_rpc_rs_rpcpool_connect_http`, Method, degree: 3)
- **.connect_simulation()** (`src_infra_rpc_rs_rpcpool_connect_simulation`, Method, degree: 3)
- **.connect_state()** (`src_infra_rpc_rs_rpcpool_connect_state`, Method, degree: 3)
- **.connect_submit()** (`src_infra_rpc_rs_rpcpool_connect_submit`, Method, degree: 2)
- **.execution_url()** (`src_infra_rpc_rs_rpcpool_execution_url`, Method, degree: 1)
- **.from_config()** (`src_infra_rpc_rs_rpcpool_from_config`, Method, degree: 1)
- **.private_url()** (`src_infra_rpc_rs_rpcpool_private_url`, Method, degree: 1)
- **.require_private_submit()** (`src_infra_rpc_rs_rpcpool_require_private_submit`, Method, degree: 1)
- **.simulation_url()** (`src_infra_rpc_rs_rpcpool_simulation_url`, Method, degree: 3)
- **.state_url()** (`src_infra_rpc_rs_rpcpool_state_url`, Method, degree: 2)
- **.submit_url()** (`src_infra_rpc_rs_rpcpool_submit_url`, Method, degree: 3)

## Relationships

- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_from_config (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_state_url (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_execution_url (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_private_url (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_require_private_submit (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_simulation_url (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_submit_url (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_connect_http (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_connect_state (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_connect_simulation (defines)
- src_infra_rpc_rs_rpcpool → src_infra_rpc_rs_rpcpool_connect_submit (defines)
- src_infra_rpc_rs_rpcpool_submit_url → src_infra_rpc_rs_rpcpool_simulation_url (calls)
- src_infra_rpc_rs_rpcpool_connect_state → src_infra_rpc_rs_rpcpool_state_url (calls)
- src_infra_rpc_rs_rpcpool_connect_state → src_infra_rpc_rs_rpcpool_connect_http (calls)
- src_infra_rpc_rs_rpcpool_connect_simulation → src_infra_rpc_rs_rpcpool_simulation_url (calls)
- src_infra_rpc_rs_rpcpool_connect_simulation → src_infra_rpc_rs_rpcpool_connect_http (calls)
- src_infra_rpc_rs_rpcpool_connect_submit → src_infra_rpc_rs_rpcpool_submit_url (calls)

