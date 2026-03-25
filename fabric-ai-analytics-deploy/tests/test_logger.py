import logging
import os
import pytest
from pathlib import Path
from deploy.logger import setup_logger


def test_setup_logger_returns_logger(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger()
    assert isinstance(logger, logging.Logger)


def test_setup_logger_creates_logs_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    setup_logger()
    assert (tmp_path / "logs").is_dir()


def test_setup_logger_creates_log_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    setup_logger()
    log_files = list((tmp_path / "logs").glob("deploy_*.log"))
    assert len(log_files) == 1


def test_logger_has_two_handlers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger()
    assert len(logger.handlers) == 2


def test_logger_file_handler_is_debug(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger()
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
    assert file_handlers[0].level == logging.DEBUG
