import pytest
from pathlib import Path

from dpdd.cli import validate_profile_args, ProfileArgs, UXError
from utils import run_profile_cli


def test_profile_args_validation(tmp_path: Path) -> None:
    # 1.1) Valid src(file.csv), dst(dir), NO FORMAT
    src_1 = tmp_path / "input.csv"
    src_1.write_text("a,b\n" + "1,2\n")
    dst_1 = tmp_path / "result"
    args_1_2 = ProfileArgs(src=src_1, dst=dst_1, fmt=None, sample=1, chunksize=10_000, topk=20)
    assert validate_profile_args(args_1_2) is None

    # 1.2) Valid src(file.parquet), dst(dir), NO FORMAT
    src_1_2 = tmp_path / "input.parquet"
    src_1_2.write_bytes(b"not a parquet file(broken)")
    args_1 = ProfileArgs(src=src_1_2, dst=dst_1, fmt=None, sample=1, chunksize=10_000, topk=20)
    assert validate_profile_args(args_1) is None

    # 1.3) Valid src(file.csv), dst(dir), format=txt(invalid)
    args_1_3 = ProfileArgs(src=src_1, dst=dst_1, fmt="txt", sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_1_3)

    # 2.1) Invalid src(file with no extension), valid dst(dir), NO FORMAT
    src_2 = tmp_path / "file_no_ext"
    src_2.write_text("123")
    args_2 = ProfileArgs(src=src_2, dst=dst_1, fmt=None, sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_2)
    
    # 2.2) invalid src(empty dir), invalid dst(dir), format=csv
    src_2_2 = tmp_path / "empty_dir"
    src_2_2.mkdir()
    args_2_2 = ProfileArgs(src=src_2_2, dst=dst_1, fmt="csv", sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_2_2)

    # 3.1) Valid src(dir), dst(dir), format=csv(valid)
    src_3 = tmp_path / "input"
    src_3.mkdir()
    (src_3 / "input.csv").write_text("a,b\n" + "1,2\n")
    args_3 = ProfileArgs(src=src_3, dst=dst_1, fmt="csv", sample=1, chunksize=10_000, topk=20)
    assert validate_profile_args(args_3) is None

    # 3.2) Valid src(dir), dst(dir), format=parquet(invalid)
    args_3_2 = ProfileArgs(src=src_3, dst=dst_1, fmt="parquet", sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_3_2)

    # 3.3) Valid src(dir), dst(dir), format=None
    args_3_3 = ProfileArgs(src=src_3, dst=dst_1, fmt=None, sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_3_3)

    # 3.4) Valid src(dir), dst(dir), format=txt(unsupported)
    args_3_4 = ProfileArgs(src=src_3, dst=dst_1, fmt="txt", sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_3_4)

    # 3.4) Valid src(dir with multiple file types), dst(dir), format=csv
    src_3_5 = tmp_path / "multiple_types"
    src_3_5.mkdir()
    (src_3_5 / "file_1.csv").write_text("a,b\n" + "1,2\n")
    (src_3_5 / "file_2.csv").write_text("a,b\n" + "1,2\n" * 2)
    (src_3_5 / "file_3.parquet").write_bytes(b"not a parquet file(broken)")
    args_3_5 = ProfileArgs(src=src_3_5, dst=dst_1, fmt="csv", sample=1, chunksize=10_000, topk=20)
    assert validate_profile_args(args_3_5) is None

    # 4.1) Valid src(file.csv), invalid dst(file), NO FORMAT
    dst_4 = tmp_path / "result.csv"
    dst_4.write_text("a,b\n" + "1,2\n")
    args_4 = ProfileArgs(src=src_1, dst=dst_4, fmt=None, sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_4)

    # 4.2) Valid src(file.csv), invalid dst(file, does not exist), NO FORMAT
    dst_4_2 = tmp_path / "result.csv"
    args_4_2 = ProfileArgs(src=src_1, dst=dst_4_2, fmt=None, sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_4_2)

    # 5) Invalid sample
    args_5 = ProfileArgs(src=src_1, dst=dst_1, fmt=None, sample=-0.4, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_5)

    args_5 = ProfileArgs(src=src_1, dst=dst_1, fmt=None, sample=1.1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_5)
    
    # 6) Invalid chunksize
    args_6 = ProfileArgs(src=src_1, dst=dst_1, fmt=None, sample=1, chunksize=-10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args_6)

    # 7) Invalid topk
    args_7 = ProfileArgs(src=src_1, dst=dst_1, fmt=None, sample=1, chunksize=10_000, topk=-10)
    with pytest.raises(UXError):
        validate_profile_args(args_7)


def test_profile_args_validation_bad_dir(tmp_path: Path, monkeypatch) -> None:
    src = tmp_path / "input.csv"
    src.write_text("a,b\n" + "1,2\n")
    dst = tmp_path / "bad_dir"

    monkeypatch.setattr("pathlib.Path.mkdir", lambda self, **kwargs: (_ for _ in ()).throw(OSError("disk full")))

    args = ProfileArgs(src=src, dst=dst, fmt=None, sample=1, chunksize=10_000, topk=20)
    with pytest.raises(UXError):
        validate_profile_args(args)
