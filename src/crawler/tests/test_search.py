import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

import search

@pytest.fixture
def query():
    return 'number_bedrooms=1&number_bathrooms=2'


@pytest.fixture
def parsed_query():
    return {'number_bedrooms':['1'], 'number_bathrooms':['2']}


@pytest.fixture
def parsed_query_with_invalid_key():
    return {'number_bedrooms':['1'], 'number_bathrooms':['2'], 'invalid': ['3']}


def test__filter_valid_query_parameters(parsed_query):
    result = search._filter_valid_query_parameters(parsed_query)
    assert result == ['number_bedrooms', 'number_bathrooms']


def test__filter_valid_query_parameters_with_invalid_key_in_query(parsed_query_with_invalid_key):
    result = search._filter_valid_query_parameters(parsed_query_with_invalid_key)
    assert result == ['number_bedrooms', 'number_bathrooms']


def test__to_query_dict(query):
    result = search._to_query_dict(query)
    assert result['number_bedrooms'] == '1'
    assert result['number_bathrooms'] == '2'
