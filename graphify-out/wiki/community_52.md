# Community 52: PipelineConfig

**Members:** 15

## Nodes

- **default_graph_rebuild_interval()** (`src_config_mod_rs_default_graph_rebuild_interval`, Function, degree: 2)
- **default_hf_max_dispatch()** (`src_config_mod_rs_default_hf_max_dispatch`, Function, degree: 2)
- **default_hf_prefetch_count()** (`src_config_mod_rs_default_hf_prefetch_count`, Function, degree: 2)
- **default_hf_score_cap()** (`src_config_mod_rs_default_hf_score_cap`, Function, degree: 2)
- **default_hf_sim_cap()** (`src_config_mod_rs_default_hf_sim_cap`, Function, degree: 2)
- **default_hf_skip_prefetch_on_stream()** (`src_config_mod_rs_default_hf_skip_prefetch_on_stream`, Function, degree: 2)
- **default_hf_trigger_on_block()** (`src_config_mod_rs_default_hf_trigger_on_block`, Function, degree: 2)
- **default_hf_trigger_on_stream()** (`src_config_mod_rs_default_hf_trigger_on_stream`, Function, degree: 2)
- **default_lf_bootstrap_batch()** (`src_config_mod_rs_default_lf_bootstrap_batch`, Function, degree: 2)
- **default_lf_full_sweep_interval()** (`src_config_mod_rs_default_lf_full_sweep_interval`, Function, degree: 2)
- **default_lf_hot_batch()** (`src_config_mod_rs_default_lf_hot_batch`, Function, degree: 2)
- **default_stream_enabled()** (`src_config_mod_rs_default_stream_enabled`, Function, degree: 2)
- **default_stream_max_pools()** (`src_config_mod_rs_default_stream_max_pools`, Function, degree: 2)
- **PipelineConfig** (`src_config_mod_rs_pipelineconfig`, Struct, degree: 2)
- **.default()** (`src_config_mod_rs_pipelineconfig_default`, Method, degree: 14)

## Relationships

- src_config_mod_rs_pipelineconfig → src_config_mod_rs_pipelineconfig_default (defines)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_lf_bootstrap_batch (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_lf_hot_batch (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_lf_full_sweep_interval (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_prefetch_count (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_score_cap (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_sim_cap (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_max_dispatch (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_graph_rebuild_interval (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_trigger_on_block (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_stream_enabled (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_stream_max_pools (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_trigger_on_stream (calls)
- src_config_mod_rs_pipelineconfig_default → src_config_mod_rs_default_hf_skip_prefetch_on_stream (calls)

