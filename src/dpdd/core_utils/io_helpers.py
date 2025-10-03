from pathlib import Path
from typing import Literal, Iterator
import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq


TRUE = {"true", "t", "1", "y", "yes"}
FALSE = {"false", "f", "0", "n", "no"}


def iter_frames(
        src: Path,
        fmt: Literal["csv", "parquet"],
        chunksize: int
) -> Iterator[tuple[Path, int, pd.DataFrame]]:
    if src.is_file():
        all_files = [src]
    elif src.is_dir():
        all_files = list(src.glob(f"*.{fmt}"))

    all_files = sorted(all_files, key=lambda x: x.name)
    for path in all_files:

        if path.stat().st_size == 0:
            continue

        chunk_idx = 0
        # csv
        if fmt == "csv":
            try:
                reader = pd.read_csv(filepath_or_buffer=path, chunksize=chunksize)
            except (pd.errors.ParserError, ValueError):
                raise

            try:
                for chunk in reader:
                    if chunk.empty or chunk.shape[1] == 0:
                        continue
                    yield path, chunk_idx, chunk
                    chunk_idx += 1
            except (pd.errors.ParserError, ValueError):
                raise

        # parquet
        elif fmt == "parquet":
            try:
               pf = pq.ParquetFile(path, memory_map=True)
            except (pa.ArrowInvalid, pa.ArrowIOError, ValueError):
                raise

            if pf.metadata.num_rows == 0:
                continue

            for rg_idx in range(pf.metadata.num_row_groups):
                rg_meta = pf.metadata.row_group(rg_idx)
                if rg_meta.num_rows == 0:
                    continue

            try:
                for chunk in pf.iter_batches(batch_size=chunksize, use_threads=True):
                    if chunk.num_rows == 0:
                        continue
                    yield path, chunk_idx, chunk.to_pandas(types_mapper=None)
                    chunk_idx += 1
            except (pa.ArrowInvalid, pa.ArrowIOError, ValueError):
                raise


def is_bool_series(s: pd.Series, treshold: float = 0.95) -> bool:
    s_clean = s.dropna()
    if s_clean.empty:
        return False
    bool_rate = s_clean.isin(TRUE | FALSE).mean()
    return bool_rate >= treshold


def is_datetime_series(s: pd.Series, threshold: float = 0.95) -> bool:
    s_clean = s.dropna()
    if s_clean.empty:
        return False
    dt_rate = pd.to_datetime(s_clean, errors="coerce").notna().mean()
    return dt_rate >= threshold


def normalize_numeric_strings(s: pd.Series) -> pd.Series:
    def _norm(x: str) -> str:
        x = x.replace(" ", "")
        x = x.replace("_", "")
        x = x.replace("\u00A0", "")
        comas = x.count(",")
        if comas > 1:
            x = x.replace(",", "")
        elif comas == 1:
            x = x.replace(",", ".")
        return x
    return s.map(_norm)


def is_string_series_numeric(s: pd.Series, threshold: float = 0.95) -> bool:
    s_clean = s.dropna().astype(str).str.strip()
    if s_clean.empty:
        return True
    s_clean = normalize_numeric_strings(s_clean)
    numeric_rate = pd.to_numeric(s_clean, errors="coerce").notna().mean()
    return numeric_rate >= threshold


def delete_overhead(profile) -> None:
    for col in profile:
        s = profile[col]
        if "original_dtype" in s:
            del profile[col]["original_dtype"]
        if "dirty" in s:
            del profile[col]["dirty"]
        if "coerce_seen" in s:
            del profile[col]["coerce_seen"]
