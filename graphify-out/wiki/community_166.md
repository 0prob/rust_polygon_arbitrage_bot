# Community 166: ReceiptPoller

**Members:** 6

## Nodes

- **fetch_receipt_from_rpc()** (`src_services_execution_receipt_rs_fetch_receipt_from_rpc`, Function, degree: 2)
- **ReceiptPoller** (`src_services_execution_receipt_rs_receiptpoller`, Struct, degree: 5)
- **.default()** (`src_services_execution_receipt_rs_receiptpoller_default`, Method, degree: 2)
- **.new()** (`src_services_execution_receipt_rs_receiptpoller_new`, Method, degree: 3)
- **.wait()** (`src_services_execution_receipt_rs_receiptpoller_wait`, Method, degree: 2)
- **.wait_with_hypersync()** (`src_services_execution_receipt_rs_receiptpoller_wait_with_hypersync`, Method, degree: 4)

## Relationships

- src_services_execution_receipt_rs_receiptpoller → src_services_execution_receipt_rs_receiptpoller_default (defines)
- src_services_execution_receipt_rs_receiptpoller → src_services_execution_receipt_rs_receiptpoller_new (defines)
- src_services_execution_receipt_rs_receiptpoller → src_services_execution_receipt_rs_receiptpoller_wait (defines)
- src_services_execution_receipt_rs_receiptpoller → src_services_execution_receipt_rs_receiptpoller_wait_with_hypersync (defines)
- src_services_execution_receipt_rs_receiptpoller_default → src_services_execution_receipt_rs_receiptpoller_new (calls)
- src_services_execution_receipt_rs_receiptpoller_wait → src_services_execution_receipt_rs_receiptpoller_wait_with_hypersync (calls)
- src_services_execution_receipt_rs_receiptpoller_wait_with_hypersync → src_services_execution_receipt_rs_fetch_receipt_from_rpc (calls)
- src_services_execution_receipt_rs_receiptpoller_wait_with_hypersync → src_services_execution_receipt_rs_receiptpoller_new (calls)

