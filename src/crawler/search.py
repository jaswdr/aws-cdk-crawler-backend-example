import logging
from datetime import datetime
from urllib.parse import parse_qs

from config import logging_level, offers_table_name
from operations import query_table
from utils import object_to_dict


log = logging.getLogger(__name__)
log.setLevel(logging_level())


VALID_QUERY_KEYS = ['number_bedrooms','number_bathrooms']


def _filter_valid_query_parameters(parsed_query: dict) -> dict:
    return list(filter(lambda k: k in VALID_QUERY_KEYS, parsed_query))


def _to_query_dict(query: dict) -> dict:
    parsed_query = parse_qs(query)
    valid_keys = _filter_valid_query_parameters(parsed_query )
    return {f: parsed_query[f].pop() for f in valid_keys}


def search_offers(query: str, table_name: str) -> list:
    user_query = _to_query_dict(query)
    result = query_table(user_query, table_name)
    return object_to_dict(result)
