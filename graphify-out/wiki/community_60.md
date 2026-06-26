# Community 60: mask_wss_url()

**Members:** 14

## Nodes

- **wss_feed** (`src_infra_wss_feed_rs`, File, degree: 16)
- **alloy::primitives::{Address, B256}** (`src_infra_wss_feed_rs_import_alloy_primitives_address_b256`, Module, degree: 1)
- **alloy::providers::{Provider, ProviderBuilder, WsConnect}** (`src_infra_wss_feed_rs_import_alloy_providers_provider_providerbuilder_wsconnect`, Module, degree: 1)
- **alloy::pubsub::Subscription** (`src_infra_wss_feed_rs_import_alloy_pubsub_subscription`, Module, degree: 1)
- **alloy::rpc::types::Filter** (`src_infra_wss_feed_rs_import_alloy_rpc_types_filter`, Module, degree: 1)
- **crate::config::AppConfig** (`src_infra_wss_feed_rs_import_crate_config_appconfig`, Module, degree: 1)
- **crate::infra::metrics::PipelineMetrics** (`src_infra_wss_feed_rs_import_crate_infra_metrics_pipelinemetrics`, Module, degree: 1)
- **crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, V2_SYNC_TOPIC, V3_SWAP_TOPIC,
}** (`src_infra_wss_feed_rs_import_crate_services_partial_cache_partialpoolcache_streamaddressset_v2_sync_topic_v3_swap_topic`, Module, degree: 1)
- **crate::util::now_ms** (`src_infra_wss_feed_rs_import_crate_util_now_ms`, Module, degree: 1)
- **futures::StreamExt** (`src_infra_wss_feed_rs_import_futures_streamext`, Module, degree: 1)
- **std::sync::Arc** (`src_infra_wss_feed_rs_import_std_sync_arc`, Module, degree: 1)
- **tokio::sync::watch** (`src_infra_wss_feed_rs_import_tokio_sync_watch`, Module, degree: 1)
- **tracing::{debug, error, info, warn}** (`src_infra_wss_feed_rs_import_tracing_debug_error_info_warn`, Module, degree: 1)
- **mask_wss_url()** (`src_infra_wss_feed_rs_mask_wss_url`, Function, degree: 1)

## Relationships

- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_std_sync_arc (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_alloy_primitives_address_b256 (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_alloy_providers_provider_providerbuilder_wsconnect (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_alloy_pubsub_subscription (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_alloy_rpc_types_filter (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_futures_streamext (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_tokio_sync_watch (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_tracing_debug_error_info_warn (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_crate_config_appconfig (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_crate_infra_metrics_pipelinemetrics (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_crate_services_partial_cache_partialpoolcache_streamaddressset_v2_sync_topic_v3_swap_topic (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_import_crate_util_now_ms (imports)
- src_infra_wss_feed_rs → src_infra_wss_feed_rs_mask_wss_url (defines)

