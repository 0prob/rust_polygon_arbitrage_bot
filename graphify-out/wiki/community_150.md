# Community 150: spawn_pool_log_feed()

**Members:** 7

## Nodes

- **http_to_wss()** (`src_infra_wss_feed_rs_http_to_wss`, Function, degree: 2)
- **PoolLogFeed** (`src_infra_wss_feed_rs_poollogfeed`, Struct, degree: 5)
- **.handle_log()** (`src_infra_wss_feed_rs_poollogfeed_handle_log`, Method, degree: 1)
- **.new()** (`src_infra_wss_feed_rs_poollogfeed_new`, Method, degree: 3)
- **.run()** (`src_infra_wss_feed_rs_poollogfeed_run`, Method, degree: 3)
- **.run_subscriptions()** (`src_infra_wss_feed_rs_poollogfeed_run_subscriptions`, Method, degree: 3)
- **spawn_pool_log_feed()** (`src_infra_wss_feed_rs_spawn_pool_log_feed`, Function, degree: 4)

## Relationships

- src_infra_wss_feed_rs_poollogfeed → src_infra_wss_feed_rs_poollogfeed_new (defines)
- src_infra_wss_feed_rs_poollogfeed → src_infra_wss_feed_rs_poollogfeed_run (defines)
- src_infra_wss_feed_rs_poollogfeed → src_infra_wss_feed_rs_poollogfeed_run_subscriptions (defines)
- src_infra_wss_feed_rs_poollogfeed → src_infra_wss_feed_rs_poollogfeed_handle_log (defines)
- src_infra_wss_feed_rs_poollogfeed_run → src_infra_wss_feed_rs_poollogfeed_run_subscriptions (calls)
- src_infra_wss_feed_rs_poollogfeed_run_subscriptions → src_infra_wss_feed_rs_poollogfeed_new (calls)
- src_infra_wss_feed_rs_spawn_pool_log_feed → src_infra_wss_feed_rs_http_to_wss (calls)
- src_infra_wss_feed_rs_spawn_pool_log_feed → src_infra_wss_feed_rs_poollogfeed_new (calls)
- src_infra_wss_feed_rs_spawn_pool_log_feed → src_infra_wss_feed_rs_poollogfeed_run (calls)

