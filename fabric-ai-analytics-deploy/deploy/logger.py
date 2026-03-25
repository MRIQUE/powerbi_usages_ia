from __future__ import annotations
import logging
import os
from datetime import datetime
from pathlib import Path

_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logger(name: str = "deploy") -> logging.Logger:
    logger = logging.getLogger(name)

    # Clear existing handlers to allow fresh setup (useful in tests)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    logger.setLevel(logging.DEBUG)

    # Console handler — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
    logger.addHandler(console)

    # File handler — DEBUG and above
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"deploy_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
    logger.addHandler(file_handler)

    return logger
