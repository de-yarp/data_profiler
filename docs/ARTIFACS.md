# 6) Форматы артефактов

## `profile.json`

```json
{
  "dataset": {
    "src": "path-or-dir",
    "format": "csv|parquet",
    "rows": 12345,
    "generated_at": "2025-08-31T10:00:00Z"
  },
  "columns": {
    "colname": {
      "type": "int|float|bool|string|datetime",
      "non_null": 12000,
      "null": 345,
      "numeric": { "min": 0, "max": 95, "mean": 36.2, "std": 12.1, "p50": 35.0, "p95": 60.0 },
      "string":  { "min_len": 1, "max_len": 120, "avg_len": 18.4, "topk": [["foo",120],["bar",80]] }
    }
  }
}
```

* Для каждого столбца указывать **только релевантную** секцию (`numeric` **или** `string` и т.д.).
* Тип определить по `pandas` dtypes; `datetime` — по `datetime64[ns]` (или явному парсингу `pd.to_datetime(..., errors="coerce")` на сэмпле).

## `drift.json`

```json
{
  "params": {
    "null_delta": 0.05,
    "mean_delta": 2.0,
    "p95_delta": 3.0
  },
  "columns": {
    "age": {
      "null_rate_left": 0.03, "null_rate_right": 0.11, "null_rate_delta": 0.08,
      "mean_left": 34.1, "mean_right": 37.0, "mean_delta": 2.9,
      "p95_left": 59.0, "p95_right": 64.0, "p95_delta": 5.0,
      "alerts": ["null_rate_exceeds_0.05", "mean_shift_gt_2.0", "p95_shift_gt_3.0"]
    }
  },
  "summary": { "alerts_total": 3 }
}
```

## `report.md` (минимум)

Markdown с:

* шапкой датасета (rows, столбцы по типам),
* для числовых — таблица метрик,
* для строк — top-k,
* если есть `drift.json` — список алертов по колонкам.