# Community 130: test_new_and_default_produce_identical_state()

**Members:** 8

## Nodes

- **.default()** (`src_services_hf_snapshot_rs_hfsnapshot_default`, Method, degree: 3)
- **SnapshotStore** (`src_services_hf_snapshot_rs_snapshotstore`, Struct, degree: 6)
- **.default()** (`src_services_hf_snapshot_rs_snapshotstore_default`, Method, degree: 5)
- **.init()** (`src_services_hf_snapshot_rs_snapshotstore_init`, Method, degree: 5)
- **.new()** (`src_services_hf_snapshot_rs_snapshotstore_new`, Method, degree: 6)
- **.publish()** (`src_services_hf_snapshot_rs_snapshotstore_publish`, Method, degree: 2)
- **.read()** (`src_services_hf_snapshot_rs_snapshotstore_read`, Method, degree: 2)
- **test_new_and_default_produce_identical_state()** (`src_services_hf_snapshot_rs_test_new_and_default_produce_identical_state`, Function, degree: 4)

## Relationships

- src_services_hf_snapshot_rs_snapshotstore → src_services_hf_snapshot_rs_snapshotstore_default (defines)
- src_services_hf_snapshot_rs_snapshotstore → src_services_hf_snapshot_rs_snapshotstore_new (defines)
- src_services_hf_snapshot_rs_snapshotstore → src_services_hf_snapshot_rs_snapshotstore_init (defines)
- src_services_hf_snapshot_rs_snapshotstore → src_services_hf_snapshot_rs_snapshotstore_read (defines)
- src_services_hf_snapshot_rs_snapshotstore → src_services_hf_snapshot_rs_snapshotstore_publish (defines)
- src_services_hf_snapshot_rs_hfsnapshot_default → src_services_hf_snapshot_rs_snapshotstore_new (calls)
- src_services_hf_snapshot_rs_hfsnapshot_default → src_services_hf_snapshot_rs_snapshotstore_default (calls)
- src_services_hf_snapshot_rs_snapshotstore_default → src_services_hf_snapshot_rs_snapshotstore_init (calls)
- src_services_hf_snapshot_rs_snapshotstore_new → src_services_hf_snapshot_rs_snapshotstore_init (calls)
- src_services_hf_snapshot_rs_snapshotstore_init → src_services_hf_snapshot_rs_snapshotstore_default (calls)
- src_services_hf_snapshot_rs_snapshotstore_init → src_services_hf_snapshot_rs_snapshotstore_new (calls)
- src_services_hf_snapshot_rs_snapshotstore_publish → src_services_hf_snapshot_rs_snapshotstore_new (calls)
- src_services_hf_snapshot_rs_test_new_and_default_produce_identical_state → src_services_hf_snapshot_rs_snapshotstore_new (calls)
- src_services_hf_snapshot_rs_test_new_and_default_produce_identical_state → src_services_hf_snapshot_rs_snapshotstore_default (calls)
- src_services_hf_snapshot_rs_test_new_and_default_produce_identical_state → src_services_hf_snapshot_rs_snapshotstore_read (calls)

