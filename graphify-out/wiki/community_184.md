# Community 184: PoolMetaFieldSelector

**Members:** 5

## Nodes

- **is_missing_graphql_field_error()** (`src_infra_hasura_rs_is_missing_graphql_field_error`, Function, degree: 2)
- **PoolMetaFieldSelector** (`src_infra_hasura_rs_poolmetafieldselector`, Struct, degree: 4)
- **.current()** (`src_infra_hasura_rs_poolmetafieldselector_current`, Method, degree: 1)
- **.degrade_for_error()** (`src_infra_hasura_rs_poolmetafieldselector_degrade_for_error`, Method, degree: 3)
- **.new()** (`src_infra_hasura_rs_poolmetafieldselector_new`, Method, degree: 1)

## Relationships

- src_infra_hasura_rs_poolmetafieldselector → src_infra_hasura_rs_poolmetafieldselector_new (defines)
- src_infra_hasura_rs_poolmetafieldselector → src_infra_hasura_rs_poolmetafieldselector_current (defines)
- src_infra_hasura_rs_poolmetafieldselector → src_infra_hasura_rs_poolmetafieldselector_degrade_for_error (defines)
- src_infra_hasura_rs_poolmetafieldselector_degrade_for_error → src_infra_hasura_rs_is_missing_graphql_field_error (calls)

