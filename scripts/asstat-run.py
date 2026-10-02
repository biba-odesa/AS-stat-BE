#!/usr/bin/env python3
"""Start receiver with a unique final report; preserve sender shutdown via exec."""
import os
from pathlib import Path
import shutil
import sys
import tempfile

if len(sys.argv) != 4:
    raise SystemExit("usage: asstat-run.py RECEIVER CONFIG REPORT_DIRECTORY")
binary, config, directory = sys.argv[1:]
root = Path(directory)
root.mkdir(parents=True, exist_ok=True)
runs = sorted((p for p in root.glob("run-*") if p.is_dir() and not p.is_symlink()), key=lambda p: p.stat().st_mtime_ns)
for old in runs[:-31]:
    shutil.rmtree(old)
run = Path(tempfile.mkdtemp(prefix="run-", dir=root))
os.execv(binary, [binary, "--config", config, "--output", str(run / "final.json")])
