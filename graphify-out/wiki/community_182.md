# Community 182: live_mode_rejects_missing_key()

**Members:** 5

## Nodes

- **base_config()** (`src_config_wallet_rs_base_config`, Function, degree: 4)
- **clears_private_key_from_config_after_load()** (`src_config_wallet_rs_clears_private_key_from_config_after_load`, Function, degree: 3)
- **dry_run_allows_missing_key()** (`src_config_wallet_rs_dry_run_allows_missing_key`, Function, degree: 3)
- **live_mode_rejects_missing_key()** (`src_config_wallet_rs_live_mode_rejects_missing_key`, Function, degree: 3)
- **.load()** (`src_config_wallet_rs_walletsecrets_load`, Method, degree: 5)

## Relationships

- src_config_wallet_rs_live_mode_rejects_missing_key → src_config_wallet_rs_base_config (calls)
- src_config_wallet_rs_live_mode_rejects_missing_key → src_config_wallet_rs_walletsecrets_load (calls)
- src_config_wallet_rs_dry_run_allows_missing_key → src_config_wallet_rs_base_config (calls)
- src_config_wallet_rs_dry_run_allows_missing_key → src_config_wallet_rs_walletsecrets_load (calls)
- src_config_wallet_rs_clears_private_key_from_config_after_load → src_config_wallet_rs_base_config (calls)
- src_config_wallet_rs_clears_private_key_from_config_after_load → src_config_wallet_rs_walletsecrets_load (calls)

