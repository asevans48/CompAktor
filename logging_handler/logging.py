'''
A set of utilities for connecting logstash to a logger handler.

Created on Jan 26, 2018

@author: simplrdev
'''

import logging
import traceback

import logstash
from elasticapm import Client


class APMConfig(object):
    """
    Elastic APM Config
    """
    app_name = None
    secret_token = None
    debug = False
    ignore_patterns = None
    server_url = None
    queue_size = 10


def add_console_handler(logger, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    """
    Add console handler

    :param logger:  The logger
    :type logger:  logging.Logger
    :param format:  The format
    :type format:  str
    """
    formatter = logging.Formatter(format)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def get_logger(name='app'):
    """
    Obtains a logger with a given name.

    :param name:  The logger name defaulting to app_logger
    :type name:  str
    :return:  The logger
    :rtype:  Logger
    """
    if name:
        return logging.getLogger(name)
    else:
        return logging.getLogger()


def add_logstash_handler(logger, log_config):
    """
    Adds a logstash handler to the given logger based on the config

    :param logger:  The logger to utilize
    :type logger:  Logger
    :param log_config:  Path to the logging configuration
    :type log_config:  LoggingConfig
    :return:  The logger to use
    :rtype:  Logger
    """
    host = log_config.host
    port = log_config.port
    version = log_config.version
    logger.addHandler(logstash.LogstashHandler(host, port, version))
    return logger


def setup_apm(apm_config):
    """
    Setup the APM

    :param apm_config:  The apm configuration class
    :type apm_config:  APMConfig
    :return:  The apm client
    :rtype:  elasticapm.Client
    """
    app_name = apm_config.app_name
    secret = apm_config.secret_token
    debug = apm_config.debug
    cdict = {
        # allowed app_name chars: a-z, A-Z, 0-9, -, _, and space from elasticapm.contrib.flask
        'APP_NAME': app_name,
        'SECRET_TOKEN': secret,
        'DEBUG': debug
    }

    ignore_patterns = apm_config.ignore_patterns
    if ignore_patterns:
        cdict['TRANSACTIONS_IGNORE_PATTERNS'] = ignore_patterns
    server_url = apm_config.server_url
    if server_url:
        cdict['SERVER_URL'] = server_url
    queue_size = apm_config.queue_size
    if queue_size:
        cdict['MAX_QUEUE_SIZE'] = queue_size
    return Client(cdict)


def log_error(logger, msg=None, apm_client=None):
    """
    Log an error

    :param logger:  The logger
    :type logger:  logging.Logger
    :param msg:  The logging message
    :type msg:  str
    :param apm_client:  The apm client if applicable
    :type apm_client:  elasticapm.Client
    """
    logger.setLevel(logging.ERROR)
    error = traceback.format_exc()
    if msg:
        error = "{}\n{}".format(msg, error)
    logger.error(traceback.format_exc())
    if apm_client:
        if msg:
            apm_client.capture_message(msg)
        apm_client.capture_exception()


def log_info(logger, msg, apm_client=None):
    """
    Log info message

    :param logger:  The logger to use
    :type logger:  logging.Logger
    :param msg:  The message to log
    :type msg:  str
    :param apm_client:  The apm client
    :type apm_client:   elasticapm.Client
    """
    logger.setLevel(logging.INFO)
    logger.info(msg)
    if apm_client:
        apm_client.capture_message(msg)


def log_warn(logger, msg, apm_client=None):
    """
    Log a warning

    :param logger:  The logger to use
    :type logger:  logging.Logger
    :param msg:  The log message
    :type msg:  str
    :param apm_client:  The apm client
    :type apm_client:  elasticapm.Client
    """
    logger.setLevel(logging.WARN)
    logger.warn(msg)
    if apm_client:
        apm_client.capture_message(msg)
