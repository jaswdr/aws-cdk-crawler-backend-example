import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

import lambda_handler
from operations import save_offers_to_ddb_table


@pytest.fixture
def context():
    return {}


@pytest.fixture
def crawler_event():
    return {}


@pytest.fixture
def enrichment_event():
    return {
            'Records': [{
                'body': '{"kind": "test", "page_url": "http://localhost", "number_bathrooms": 2, "number_bedrooms": 1, "description": "Test Description", "display_address": "Test Address", "features": ["Test Feature 1", "Test Feature 2"], "manager_id": "Test Manager", "photos": ["http://localhost"], "price": 1000, "price_period": "month", "number_of_views": 7130, "latitude": 53.34405828419555, "longitude": -6.234840952683702, "publish_date": "01/04/2021", "collected_at": 1617306018.176352}'}]}


@pytest.fixture
def search_event():
    return {'rawQueryString': 'number_bedrooms=2&number_bathrooms=1'}


def test_crawler(crawler_event, context, offers_table, offers_queue_url, monkeypatch):
    monkeypatch.setenv('APP_OFFERS_TABLE', offers_table)
    monkeypatch.setenv('APP_OFFERS_QUEUE_URL', offers_queue_url)
    result = lambda_handler.crawler(crawler_event, context)
    assert result['status_code'] == 200
    assert result['total_collected']


def test_enrichment(enrichment_event, context, offers_table, monkeypatch):
    monkeypatch.setenv('APP_OFFERS_TABLE', offers_table)
    result = lambda_handler.enrichment(enrichment_event, context)
    assert result['status_code'] == 200
    assert result['total_enriched'] == 1


def test_search(search_event, context, offer, offers_table, monkeypatch):
    monkeypatch.setenv('APP_OFFERS_TABLE', offers_table)
    save_offers_to_ddb_table([offer], offers_table)
    result = lambda_handler.search(search_event, context)
    assert result['status_code'] == 200
    assert len(result['offers'])
