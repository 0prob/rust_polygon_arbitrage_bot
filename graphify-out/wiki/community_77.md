# Community 77: CircuitBreaker

**Members:** 12

## Nodes

- **auto_resets_after_cooldown()** (`src_services_execution_circuit_breaker_rs_auto_resets_after_cooldown`, Function, degree: 3)
- **CircuitBreaker** (`src_services_execution_circuit_breaker_rs_circuitbreaker`, Struct, degree: 11)
- **.check_operator_balance()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_check_operator_balance`, Method, degree: 4)
- **.default()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_default`, Method, degree: 2)
- **.is_paused()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_is_paused`, Method, degree: 3)
- **.new()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_new`, Method, degree: 3)
- **.pause_reason()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_pause_reason`, Method, degree: 1)
- **.record_failure()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_record_failure`, Method, degree: 2)
- **.record_success()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_record_success`, Method, degree: 1)
- **.reset()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_reset`, Method, degree: 2)
- **.trip()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_trip`, Method, degree: 4)
- **.try_auto_reset()** (`src_services_execution_circuit_breaker_rs_circuitbreaker_try_auto_reset`, Method, degree: 4)

## Relationships

- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_default (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_new (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_is_paused (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_pause_reason (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_trip (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_reset (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_try_auto_reset (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_record_success (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_record_failure (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker → src_services_execution_circuit_breaker_rs_circuitbreaker_check_operator_balance (defines)
- src_services_execution_circuit_breaker_rs_circuitbreaker_default → src_services_execution_circuit_breaker_rs_circuitbreaker_new (calls)
- src_services_execution_circuit_breaker_rs_circuitbreaker_try_auto_reset → src_services_execution_circuit_breaker_rs_circuitbreaker_is_paused (calls)
- src_services_execution_circuit_breaker_rs_circuitbreaker_try_auto_reset → src_services_execution_circuit_breaker_rs_circuitbreaker_reset (calls)
- src_services_execution_circuit_breaker_rs_circuitbreaker_record_failure → src_services_execution_circuit_breaker_rs_circuitbreaker_trip (calls)
- src_services_execution_circuit_breaker_rs_circuitbreaker_check_operator_balance → src_services_execution_circuit_breaker_rs_circuitbreaker_trip (calls)
- src_services_execution_circuit_breaker_rs_circuitbreaker_check_operator_balance → src_services_execution_circuit_breaker_rs_circuitbreaker_is_paused (calls)
- src_services_execution_circuit_breaker_rs_circuitbreaker_check_operator_balance → src_services_execution_circuit_breaker_rs_circuitbreaker_try_auto_reset (calls)
- src_services_execution_circuit_breaker_rs_auto_resets_after_cooldown → src_services_execution_circuit_breaker_rs_circuitbreaker_new (calls)
- src_services_execution_circuit_breaker_rs_auto_resets_after_cooldown → src_services_execution_circuit_breaker_rs_circuitbreaker_trip (calls)

