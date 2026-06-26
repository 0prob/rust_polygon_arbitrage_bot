# Community 181: NoopUiHook

**Members:** 5

## Nodes

- **NoopUiHook** (`src_orchestrator_ui_hook_rs_noopuihook`, Struct, degree: 5)
- **.on_gas_update()** (`src_orchestrator_ui_hook_rs_noopuihook_on_gas_update`, Method, degree: 1)
- **.on_hf_tick()** (`src_orchestrator_ui_hook_rs_noopuihook_on_hf_tick`, Method, degree: 1)
- **.on_lf_complete()** (`src_orchestrator_ui_hook_rs_noopuihook_on_lf_complete`, Method, degree: 1)
- **PipelineUiHook** (`src_orchestrator_ui_hook_rs_pipelineuihook`, Trait, degree: 2)

## Relationships

- src_orchestrator_ui_hook_rs_noopuihook → src_orchestrator_ui_hook_rs_pipelineuihook (implements)
- src_orchestrator_ui_hook_rs_noopuihook → src_orchestrator_ui_hook_rs_noopuihook_on_lf_complete (defines)
- src_orchestrator_ui_hook_rs_noopuihook → src_orchestrator_ui_hook_rs_noopuihook_on_hf_tick (defines)
- src_orchestrator_ui_hook_rs_noopuihook → src_orchestrator_ui_hook_rs_noopuihook_on_gas_update (defines)

