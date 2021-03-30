from datetime import datetime
import pytest

@pytest.fixture
def offer():
    return {
        'kind': 'test',
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
