from datetime import datetime, timezone
import pandas as pd
from pathlib import Path
import json
import os

from collections import Counter
from collections.abc import Hashable
from typing import Any
from pandas._typing import DtypeObj
from dataclasses import dataclass
from math import sqrt

from .core_utils.io_helpers import (delete_overhead,
                                    iter_frames,
                                    is_bool_series,
                                    is_datetime_series,
                                    is_string_series_numeric, normalize_numeric_strings, TRUE, FALSE)
from dpdd.log_json import time_now_iso

THRESHOLD = 0.95

@dataclass
class ProfileArgs:
    src: Path
    dst: Path
    fmt: str | None
    sample: float
    chunksize: int
    topk: int
    threshold: float


type StatDTypeScalar = float | int | str | datetime | Counter[Hashable]
type StatDict   = dict[str, "Stat"]
type Stat       = StatDTypeScalar | StatDict


def _init_column_state(name: str, dt: DtypeObj, s: pd.Series) -> StatDict:
    stat: StatDict = {}
    extra: Stat = {}
    stat["original_dtype"] = str(dt)
    stat["non_null"] = 0
    stat["null"] = 0

    if pd.api.types.is_bool_dtype(dt) or is_bool_series(s, THRESHOLD):
        # булевая колонка
        stat["type"] = "bool"
        extra["true_count"] = 0
        extra["false_count"] = 0
        extra["true_rate"] = -1.0

    elif pd.api.types.is_numeric_dtype(dt):
        # числовая колонка
        stat["type"] = "numeric"
        extra["s"] = 0.0
        extra["s2"] = 0.0
        extra["min"] = float("inf")
        extra["max"] = float("-inf")

    elif pd.api.types.is_string_dtype(dt) or dt == object:
        # строковая колонка

        if is_datetime_series(s, THRESHOLD):
            stat["type"] = "datetime"
            extra["min_dt"] = datetime.max.replace(tzinfo=timezone.utc)
            extra["max_dt"] = datetime.min.replace(tzinfo=timezone.utc)

        elif is_bool_series(s, THRESHOLD):
            stat["type"] = "bool"
            extra["true_count"] = 0
            extra["false_count"] = 0
            extra["true_rate"] = -1.0

        elif is_string_series_numeric(s, THRESHOLD):
            stat["type"] = "numeric"
            stat["dirty"] = True
            extra["s"] = 0.0
            extra["s2"] = 0.0
            extra["min"] = float("inf")
            extra["max"] = float("-inf")

        else:
            stat["type"] = "string"
            extra["sum_len"] = 0
            extra["min_len"] = float("inf")
            extra["max_len"] = float("-inf")
            extra["counter"] = Counter()

    elif pd.api.types.is_datetime64_any_dtype(dt) or is_datetime_series(s, THRESHOLD):
        # datetime колонка
        stat["type"] = "datetime"
        extra["min_dt"] = datetime.max.replace(tzinfo=timezone.utc)
        extra["max_dt"] = datetime.min.replace(tzinfo=timezone.utc)

    else:
        extra = {"msg": "unexpected column dtype"}

    if stat["type"] == "int" or stat["type"] == "float":
        stat["numeric"] = extra
    else:
        stat[stat["type"]] = extra

    return stat


def _init_df_profile_state(df: pd.DataFrame) -> dict[str, StatDict]:
    profile: dict[str, StatDict] = {}
    for col in df.columns:
        profile[col] = _init_column_state(col, df[col].dtype, df[col])

    return profile


def update_profile(profile: dict[str, Any], df: pd.DataFrame, emit) -> None:
    for col in df.columns:
        s = df[col]
        stat = profile[col]
        dirty_numeric_string: bool = is_string_series_numeric(s, THRESHOLD)
        dirty = stat.get("dirty", False)
        non_null_inc = int(s.notna().sum())
        stat["non_null"] += non_null_inc
        stat["null"] += len(s) - non_null_inc
        # extra = stat["extra"]

        s_clean = s.copy().dropna()

        if dirty_numeric_string:
            if "dirty" not in stat and stat["original_dtype"] == "object":
                stat["dirty"] = True
            stat["type"] = "numeric"
            if "numeric" not in stat:
                extra = {}
                extra["s"] = 0.0
                extra["s2"] = 0.0
                extra["min"] = float("inf")
                extra["max"] = float("-inf")
                stat["numeric"] = extra
            if "coercion" not in stat and stat["original_dtype"] == "object":
                extra = {}
                extra["kind"] = "numeric"
                extra["coerced_nulls"] = 0
                extra["total"] = 0
                extra["rate"] = 0.0
                stat["coercion"] = extra

        # ------------- ЧАСТЬ С ДОБАВЛЕНИЕМ МЕТРИКИ STRING НА MIXED КОЛОНКУ -------------
        # if dirty and not dirty_numeric_string:
        #     stat["type"] = "string"
        #     if "string" not in stat:
        #         extra = {}
        #         extra["sum_len"] = 0
        #         extra["min_len"] = float("inf")
        #         extra["max_len"] = float("-inf")
        #         extra["counter"] = Counter()
        #         stat["string"] = extra

        if stat["type"] == "numeric":
            # числовая колонка
            extra = stat["numeric"]
            if "int" in stat["original_dtype"]:
                stat["type"] = "int"
            elif "float" in stat["original_dtype"]:
                stat["type"] = "float"
            if dirty:
                s_clean = normalize_numeric_strings(s_clean)
                non_null_coerce = pd.to_numeric(s_clean, errors="coerce").notna().sum()
                total = len(s_clean)
                nulls_coerced = int(total - non_null_coerce)
                rate = non_null_coerce / total
                if not stat.get("coerce_seen", False):
                    emit(level="WARN",
                        event="profile_column_coercion",
                        column=col,
                        kind=stat["type"],
                        coerced_nulls=nulls_coerced,
                        total=total,
                        rate=rate)
                    stat["coerce_seen"] = True
                coercion = stat["coercion"]
                coercion["total"] += total
                coercion["coerced_nulls"] += nulls_coerced
                stat["non_null"] -= nulls_coerced
                stat["null"] += nulls_coerced
                if coercion["total"] > 0:
                    coercion["rate"] = non_null_coerce / coercion["total"]
                s_clean = pd.to_numeric(s_clean, errors="coerce").dropna()
                stat["type"] = "float"
                # del stat["detected_from_string"]
            extra["s"] += s_clean.sum()
            extra["s2"] += (s_clean ** 2).sum()
            extra["min"] = min(extra["min"], s_clean.min())
            extra["max"] = max(extra["max"], s_clean.max())

        elif stat["type"] == "string":
            # строковая колонка
            extra = stat["string"]
            s_clean = s_clean.astype(str)
            vc = s_clean.value_counts().to_dict()
            s_clean = s_clean.str.len()

            extra["sum_len"] += s_clean.sum()
            extra["min_len"] = min(extra["min_len"], s_clean.min())
            extra["max_len"] = max(extra["max_len"], s_clean.max())
            extra["counter"].update(vc)

        elif stat["type"] == "datetime":
            # datetime колонка
            extra = stat["datetime"]
            sc = pd.to_datetime(s_clean, errors="coerce", utc=True).dropna()
            extra["min_dt"] = min(extra["min_dt"], sc.min())
            extra["max_dt"] = max(extra["max_dt"], sc.max())

        elif stat["type"] == "bool":
            # булевая колонка
            extra = stat["bool"]
            if not pd.api.types.is_bool_dtype(s_clean.dtype):
                true_count_inc = int(s_clean.str.lower().isin(TRUE | FALSE).sum())
            true_count_inc = int(s_clean.sum())
            extra["true_count"] += true_count_inc
            extra["false_count"] += len(s_clean) - true_count_inc


def get_advanced_metrics(profile: dict[str, Any], k: int) -> None:
    def _to_iso(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt.replace(tzinfo=timezone.utc)
        return (dt
                .astimezone(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
                )

    for col in profile:
        stat = profile[col]
        # if stat["type"] == "int" or stat["type"] == "float":
        #     extra = stat["numeric"]
        # else:
            # extra = stat[stat["type"]]
        extra = stat.get(stat["type"], None)
        non_null = stat["non_null"]

        # if stat["type"] == "float" or stat["type"] == "int":
        if "numeric" in stat:
            extra = stat["numeric"]
            mean = None
            std = None
            if non_null > 0:
                mean = extra["s"] / non_null
                std = sqrt(max(extra["s2"] / non_null - mean**2, 0.0))
            extra["mean"] = mean
            extra["std"] = std
            del extra["s"], extra["s2"]

        # if stat["type"] == "string":
        if "string" in stat:
            extra = stat["string"]
            avg_len = None
            if non_null > 0:
                avg_len = extra["sum_len"] / non_null
            extra["avg_len"] = avg_len
            top_k = extra["counter"].most_common(k)
            extra["top_k"] = top_k if top_k else None
            del extra["sum_len"], extra["counter"]

        if stat["type"] == "bool":
            true_rate = None
            if non_null > 0:
                true_rate = extra["true_count"] / non_null
            extra["true_rate"] = true_rate

        if stat["type"] == "datetime":
            extra["min"] = _to_iso(extra["min_dt"])
            extra["max"] = _to_iso(extra["max_dt"])
            del extra["min_dt"], extra["max_dt"]


def run_profile(args: ProfileArgs, emit) -> int:
    emit(level="INFO",
         event="profile_started",
         src=str(args.src),
         format=args.fmt,
         sample=args.sample,
         chunksize=args.chunksize,
         topk=args.topk
         )

    global THRESHOLD
    THRESHOLD = args.threshold

    rows_total = 0
    columns_max = 0
    profile: dict[str, Any] = {}
    try:
        frames = iter_frames(args.src, args.fmt, args.chunksize)
        for path, chunk_idx, df in frames:
            if chunk_idx == 0:
                emit(level="INFO",
                     event="profile_file_started",
                     path=str(path))

            if not profile:
                profile = _init_df_profile_state(df)

            for col in df.columns:
                if col not in profile:
                    profile[col] = _init_column_state(col, df[col].dtype, df[col])

            rows_total += len(df)
            columns_max = max(columns_max, df.shape[1])

            update_profile(profile, df, emit)

            emit(level="INFO",
                 event="profile_chunk_scanned",
                 path=str(path),
                 chunk_idx=chunk_idx,
                 rows=len(df))

    except Exception as e:
        emit(level="ERROR",
             event="profile_failed",
             exception_type=type(e).__name__,
             exception_msg=str(e))
        return 4

    get_advanced_metrics(profile, args.topk)
    delete_overhead(profile)

    metrics = {
            "dataset": {
                "src": str(args.src),
                "format": args.fmt,
                "rows": rows_total,
                "generated_at": time_now_iso()
            },
            "columns": profile
            }

    tmp = args.dst / "profile.json.tmp"
    final = args.dst / "profile.json"
    metrics_json = json.dumps(metrics, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    try:
        tmp.write_text(metrics_json)
        os.replace(tmp, final)
    except OSError as e:
        emit(level="ERROR",
             event="profile_failed",
             exception_type=type(e).__name__,
             exception_msg=str(e))

        return 3

    emit(level="INFO",
         event="profile_completed",
         rows_total=rows_total,
         columns=columns_max,
         out_path=str(final))

    return 0
