# 5) Лог-контракт (строгий JSON в stdout)

Общие поля для каждой записи:
`ts (ISO8601 Z)`, `level ∈ {DEBUG, INFO, WARN, ERROR}`, `event (str)`, `run_id (uuid4)`, `component (str)`, `...payload (плоский)`.

## События `profile`

* `profile_started` — `{src, format, sample, chunksize, topk}`
* `profile_file_started` — `{path}`
* `profile_chunk_scanned` — `{path, chunk_idx, rows}`
* `profile_completed` — `{rows_total, columns, out_path}`
* `profile_failed` (ERROR) — `{exception_type, exception_msg}`

## События `compare`

* `compare_started` — `{left_path, right_path, thresholds}`
* `compare_alert` (WARN) — `{column, kind, value, threshold}`  *(для каждого срабатывания)*
* `compare_completed` — `{alerts_total, out_path}`
* `compare_failed` (ERROR) — `{exception_type, exception_msg}`

## События `report`

* `report_started` — `{profile_path, drift_path?, fmt}`
* `report_completed` — `{out_path}`
* `report_failed` (ERROR) — `{exception_type, exception_msg}`

## Ретраи (общие)

* `convert_retry` (WARN) — `{attempt, sleep_ms, exception_type, exception_msg}`