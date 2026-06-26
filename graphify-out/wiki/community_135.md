# Community 135: FlashLiquidityCache

**Members:** 8

## Nodes

- **collect_flash_tokens()** (`src_services_execution_flash_liquidity_rs_collect_flash_tokens`, Function, degree: 3)
- **decode_balance()** (`src_services_execution_flash_liquidity_rs_decode_balance`, Function, degree: 2)
- **FlashLiquidityCache** (`src_services_execution_flash_liquidity_rs_flashliquiditycache`, Struct, degree: 6)
- **.default()** (`src_services_execution_flash_liquidity_rs_flashliquiditycache_default`, Method, degree: 3)
- **.new()** (`src_services_execution_flash_liquidity_rs_flashliquiditycache_new`, Method, degree: 5)
- **.refresh()** (`src_services_execution_flash_liquidity_rs_flashliquiditycache_refresh`, Method, degree: 3)
- **.snapshot()** (`src_services_execution_flash_liquidity_rs_flashliquiditycache_snapshot`, Method, degree: 1)
- **.with_addresses()** (`src_services_execution_flash_liquidity_rs_flashliquiditycache_with_addresses`, Method, degree: 2)

## Relationships

- src_services_execution_flash_liquidity_rs_flashliquiditycache → src_services_execution_flash_liquidity_rs_flashliquiditycache_default (defines)
- src_services_execution_flash_liquidity_rs_flashliquiditycache → src_services_execution_flash_liquidity_rs_flashliquiditycache_new (defines)
- src_services_execution_flash_liquidity_rs_flashliquiditycache → src_services_execution_flash_liquidity_rs_flashliquiditycache_with_addresses (defines)
- src_services_execution_flash_liquidity_rs_flashliquiditycache → src_services_execution_flash_liquidity_rs_flashliquiditycache_snapshot (defines)
- src_services_execution_flash_liquidity_rs_flashliquiditycache → src_services_execution_flash_liquidity_rs_flashliquiditycache_refresh (defines)
- src_services_execution_flash_liquidity_rs_flashliquiditycache_default → src_services_execution_flash_liquidity_rs_flashliquiditycache_new (calls)
- src_services_execution_flash_liquidity_rs_flashliquiditycache_with_addresses → src_services_execution_flash_liquidity_rs_flashliquiditycache_new (calls)
- src_services_execution_flash_liquidity_rs_flashliquiditycache_refresh → src_services_execution_flash_liquidity_rs_flashliquiditycache_new (calls)
- src_services_execution_flash_liquidity_rs_flashliquiditycache_refresh → src_services_execution_flash_liquidity_rs_decode_balance (calls)
- src_services_execution_flash_liquidity_rs_collect_flash_tokens → src_services_execution_flash_liquidity_rs_flashliquiditycache_default (calls)
- src_services_execution_flash_liquidity_rs_collect_flash_tokens → src_services_execution_flash_liquidity_rs_flashliquiditycache_new (calls)

