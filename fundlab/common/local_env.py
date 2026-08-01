"""Load optional repository-local environment values without overriding the process."""

from __future__ import annotations

import ast
import os
from pathlib import Path
import re


_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def load_local_environment(path: str | Path = ".env.local") -> None:
    """Load a small dotenv-compatible file, preserving explicit environment values."""

    source = Path(path)
    if not source.is_file():
        return
    for line_number, raw_line in enumerate(
        source.read_text(encoding="utf-8-sig").splitlines(), start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"Invalid local environment entry at {source}:{line_number}")
        name, raw_value = line.split("=", 1)
        name = name.strip()
        if not _ENVIRONMENT_NAME.fullmatch(name):
            raise ValueError(f"Invalid environment name at {source}:{line_number}")
        if not name.startswith("FUNDLAB_"):
            raise ValueError(f"Unsupported local environment name at {source}:{line_number}")
        value = raw_value.strip()
        if value.startswith(("\"", "'")):
            try:
                parsed = ast.literal_eval(value)
            except (SyntaxError, ValueError) as exc:
                raise ValueError(
                    f"Invalid quoted environment value at {source}:{line_number}",
                ) from exc
            if not isinstance(parsed, str):
                raise ValueError(
                    f"Quoted environment value is not text at {source}:{line_number}",
                )
            value = parsed
        os.environ.setdefault(name, value)
