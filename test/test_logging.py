"""
Logging test.

@author aevans
"""
import logging

from logging_handler import logging as app_logger
from logging_handler.logging import add_console_handler


def test_log_inf():
    logger = app_logger.get_logger('info')
    add_console_handler(logger)
    app_logger.log_info(logger, 'Hello World')


def test_log_error():
    logger = app_logger.get_logger('Error')
    add_console_handler(logger)
    app_logger.log_error(logger, 'Hello World')


def test_log_warn():
    logger = app_logger.get_logger('Warn')
    add_console_handler(logger)
    app_logger.log_warn(logger, 'Hello World')
    app_logger.log_info(logger, 'Hello World')
