import logging
import os


def logging_level():
    return os.environ.get('APP_LOGGING_LEVEL', logging.DEBUG)


def offers_table_name():
    return os.environ.get('APP_OFFERS_TABLE')


def offers_queue_url():
    return os.environ.get('APP_OFFERS_QUEUE_URL')
