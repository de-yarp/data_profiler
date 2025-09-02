import typer
from typing import Annotated, Optional
from pathlib import Path
import uuid

from dataclasses import dataclass


app = typer.Typer(add_completion=False, rich_markup_mode="markdown")


class UXError(Exception):
    """User input error (arguments validation, UX)."""
    pass


@dataclass
class ProfileArgs:
    src: Path
    dst: Path
    fmt: str | None
    sample: float
    chunksize: int
    topk: int


def dir_get_suffix(src: Path) -> set[str]:
    return set(s.suffix[1:] for s in src.iterdir() if s.is_file())


def detect_format(src: Path, fmt: str | None) -> str:
    if src.is_file():
        if fmt:
            if fmt == src.suffix[1:].lower():
                return fmt
            else:
                raise UXError(f"ERR: invalid format [{fmt}] for a file with ext - [{src.suffix[1:]}]")
        else:
            return src.suffix[1:].lower()
    elif src.is_dir():
        if not fmt:
            raise UXError("ERR: --format required for directory")
        return fmt
    else:
        raise UXError(f"ERR: src not found - {src}")


def validate_profile_args(args: ProfileArgs) -> None:
    if not args.src.exists():
        raise UXError(f"ERR: src not found")

    if args.dst.exists() and args.dst.is_file():
        raise UXError(f"ERR: dst must be a directory")
    if not args.dst.exists() and args.dst.suffix:
        raise UXError(f"ERR: dst must be a directory")
    try:
        args.dst.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise UXError(f"ERR: could not create dst - {type(e).__name__}")

    if not 0 < args.sample <= 1:
        raise UXError(f"ERR: sample must be in (0; 1]")

    allowed_fmts = {"csv", "parquet"}
    fmt = detect_format(args.src, args.fmt)
    if fmt not in allowed_fmts:
        raise UXError(f"ERR: unsupported format - {fmt}")
    if args.src.is_dir():
        src_fmts = dir_get_suffix(args.src)
        if fmt not in src_fmts:
            raise UXError(f"ERR: src does not contain files of format - {fmt}")

    if args.chunksize <= 0:
        raise UXError(f"ERR: chunksize must be >0")

    if args.topk <= 0:
        raise UXError(f"ERR: topk must be >0")

@app.command(name="profile")
def profile(
    src: Path = typer.Option(..., "--src", help="input file or directory"),
    dst: Path = typer.Option(..., "--dst", help="output directory"),
    fmt: Optional[str] = typer.Option(None, "--format", "-f", help="csv|parquet (required for dir)"),
    sample: float = typer.Option(1.0, "--sample", help="(0;1]"),
    chunksize: int = typer.Option(10_000, "--chunksize", help="rows per chunk/batch"),
    topk: int = typer.Option(20, "--topk", help="top-K frequent values"),
) -> None:
    args = ProfileArgs(src=src, dst=dst, fmt=fmt, sample=sample, chunksize=chunksize, topk=topk)
    try:
        validate_profile_args(args)
    except UXError as e:
        typer.secho(f"Error: {e}", err=True)
        raise typer.Exit(2)
    pass

def main() -> None:
    app()


if __name__ == "__main__":
    main()
