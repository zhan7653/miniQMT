from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_dir: str | Path = "logs", level: int = logging.INFO) -> None:
    directory = Path(log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / "fundlab.log"

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )

