import pytest
from datetime import datetime
from unittest.mock import patch

import operations


def test__get_ddb_client():
    client = operations._get_ddb_client()
    assert client.__class__.__name__ == 'DynamoDB'

def test__pk(offer):
    assert operations._pk(offer) == 'offer#test'


def test__sk(offer):
    assert operations._sk(offer) == '2#1#1000#test-address'


def test__slug(offer):
    assert operations._slug('Foo bar') == 'foo-bar'


def test__ttl():
    ttl = operations._ttl()
    assert isinstance(ttl, str)
    ttl_int = int(ttl)
    assert ttl_int > 0


def test__put_requests_from_offers(offer):
    put_requests = operations._put_requests_from_offers([offer])
    assert len(put_requests) == 1
    put_request = put_requests[0]['PutRequest']['Item']
    assert put_request['PK']
    assert put_request['SK']
    assert put_request['TTL']
