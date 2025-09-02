import subprocess
import sys


def run_profile_cli(*args):
    p = subprocess.run(
        [sys.executable, "-m", "src.dpdd.cli", *args],
        capture_output=True,
        text=True
    )
    return p.returncode, p.stdout.splitlines(), p.stderr
