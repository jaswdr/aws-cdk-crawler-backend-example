from datetime import datetime
import pytest
import boto3

from operations import KIND_APARTMENT

@pytest.fixture
def offer():
    return {
        'kind': f'{KIND_APARTMENT}-test',
        'page_url': 'http://localhost',
        'number_bathrooms': 1,
        'number_bedrooms': 2,
        'description': 'Test Description',
        'display_address': 'Test Address',
        'features': ['Parking', 'Central Heating'],
        'manager_id': 'Laura Doe',
        'photos': ['http://localhost'],
        'price': 1000,
        'price_period': 'month',
        'number_of_views': 500,
        'publish_date': '03/03/2021',
        'collected_at': datetime.utcnow().timestamp(),
        'latitude': 1.0,
        'longitude': 2.0
    }


@pytest.fixture
def offers_table():
    return 'Offers'


@pytest.fixture(scope='session')
def offers_queue_url():
    sqs = boto3.client('sqs')
    response = sqs.get_queue_url(QueueName='CrawlerEnrichmentQueue')
    return response['QueueUrl']
