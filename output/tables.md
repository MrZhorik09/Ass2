# Generated results

Run: 2026-10-02T19:17:54 | source: fault_tolerant_processor.py | commit: 871f0e6 | Python 3.11.15

## Part A - Baseline
| Metric | Result |
|---|---|
| Transactions attempted | 2 |
| Successful transactions | 1 |
| Transactions lost | 14 |
| Processed amount (KZT) | 12 000 |
| Lost amount (KZT) | 425 000 |

Stopped at T002: `Network fault during Process for T002 (Initial)`

## Part B - Exception handling
| Transaction | Failure | Exception | Recovery Action | Final Status |
|---|---|---|---|---|
| T002 | Network | NetworkFault (Process) | Caught, classified, logged; transaction skipped; continue | FAILED |
| T004 | Timeout | TimeoutFault (Process) | Caught, classified, logged; transaction skipped; continue | FAILED |
| T006 | Database | DatabaseFault (Record) | Caught, classified, logged; transaction skipped; continue | FAILED |
| T008 | Network | NetworkFault (Process) | Caught, classified, logged; transaction skipped; continue | FAILED |
| T010 | Timeout | TimeoutFault (Process) | Caught, classified, logged; transaction skipped; continue | FAILED |
| T012 | Database | DatabaseFault (Record) | Caught, classified, logged; transaction skipped; continue | FAILED |
| T015 | Network | NetworkFault (Process) | Caught, classified, logged; transaction skipped; continue | FAILED |

Successful 8, failed 7, confirmed amount 96 000 KZT. Ledger total without rollback: 246 000 KZT (dirty rows: T006, T012).

## Part C - Retry attempt log
| Step | Timestamp | Transaction | Attempt | Failure | Action | Result |
|---|---|---|---|---|---|---|
| 1 | 2026-10-02T19:17:54.086 | T001 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 2 | 2026-10-02T19:17:54.086 | T002 | Initial | Network | NetworkFault in Process; backoff 0.01s, retry | FAIL |
| 3 | 2026-10-02T19:17:54.097 | T002 | Retry 1 | None | Validate->Process->Record committed | SUCCESS |
| 4 | 2026-10-02T19:17:54.097 | T003 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 5 | 2026-10-02T19:17:54.097 | T004 | Initial | Timeout | TimeoutFault in Process; backoff 0.01s, retry | FAIL |
| 6 | 2026-10-02T19:17:54.107 | T004 | Retry 1 | Timeout | TimeoutFault in Process; backoff 0.02s, retry | FAIL |
| 7 | 2026-10-02T19:17:54.127 | T004 | Retry 2 | None | Validate->Process->Record committed | SUCCESS |
| 8 | 2026-10-02T19:17:54.127 | T005 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 9 | 2026-10-02T19:17:54.128 | T006 | Initial | Database | DatabaseFault in Record; backoff 0.01s, retry | FAIL |
| 10 | 2026-10-02T19:17:54.138 | T006 | Retry 1 | Database | DatabaseFault in Record; backoff 0.02s, retry | FAIL |
| 11 | 2026-10-02T19:17:54.158 | T006 | Retry 2 | Database | Retries exhausted; rollback (undo own ledger write) | ROLLBACK |
| 12 | 2026-10-02T19:17:54.158 | T007 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 13 | 2026-10-02T19:17:54.158 | T008 | Initial | Network | NetworkFault in Process; backoff 0.01s, retry | FAIL |
| 14 | 2026-10-02T19:17:54.169 | T008 | Retry 1 | None | Validate->Process->Record committed | SUCCESS |
| 15 | 2026-10-02T19:17:54.169 | T009 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 16 | 2026-10-02T19:17:54.169 | T010 | Initial | Timeout | TimeoutFault in Process; backoff 0.01s, retry | FAIL |
| 17 | 2026-10-02T19:17:54.179 | T010 | Retry 1 | Timeout | TimeoutFault in Process; backoff 0.02s, retry | FAIL |
| 18 | 2026-10-02T19:17:54.200 | T010 | Retry 2 | None | Validate->Process->Record committed | SUCCESS |
| 19 | 2026-10-02T19:17:54.200 | T011 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 20 | 2026-10-02T19:17:54.200 | T012 | Initial | Database | DatabaseFault in Record; backoff 0.01s, retry | FAIL |
| 21 | 2026-10-02T19:17:54.210 | T012 | Retry 1 | Database | DatabaseFault in Record; backoff 0.02s, retry | FAIL |
| 22 | 2026-10-02T19:17:54.231 | T012 | Retry 2 | Database | Retries exhausted; rollback (undo own ledger write) | ROLLBACK |
| 23 | 2026-10-02T19:17:54.231 | T013 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 24 | 2026-10-02T19:17:54.231 | T014 | Initial | None | Validate->Process->Record committed | SUCCESS |
| 25 | 2026-10-02T19:17:54.231 | T015 | Initial | Network | NetworkFault in Process; backoff 0.01s, retry | FAIL |
| 26 | 2026-10-02T19:17:54.241 | T015 | Retry 1 | None | Validate->Process->Record committed | SUCCESS |

Retries: 11, rollbacks: 2, duplicate writes suppressed: 4

## Part D - Checkpoints
| Checkpoint | Successful transactions | Total amount (KZT) | State saved |
|---|---|---|---|
| CP1 | 5 | 103 000 | output/checkpoints/CP1.json: T001…T005, last=T005 |
| CP2 | 10 | 214 000 | output/checkpoints/CP2.json: T001…T011, last=T011 |
| CP3 (final (end of batch)) | 13 | 287 000 | output/checkpoints/CP3.json: T001…T015, last=T015 |

## Part E - Before/after
| Metric | Baseline | Fault-tolerant | Improvement |
|---|---|---|---|
| Successful transactions | 1 | 13 | +12 |
| Failed transactions | 14 | 2 | -12 |
| Lost amount (KZT) | 425 000 | 150 000 | -275 000 (64.71%) |
| Retries | 0 | 11 | 11 faults absorbed by retry attempts |
| Rollbacks | 0 | 2 | ledger kept consistent |
| Completion rate | 6.67% | 86.67% | +80.00 pp |

Recovery improvement = 86.67% - 6.67% = +80.00 pp

Transaction-loss reduction = (14 - 2) / 14 = 85.71%

Retry recovered 5 of 7 faulted transactions (71.43%).
