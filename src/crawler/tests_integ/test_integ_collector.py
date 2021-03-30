import pytest
from unittest.mock import patch

import collector

def test_collect_apartments_page_zero():
    apartments = collector.collect_apartments_page(0, limit=1)
    assert len(apartments) == 1
    apartment = apartments[0]
    assert apartment['collected_at']
    assert apartment['description']
    assert apartment['display_address']
    assert len(apartment['features'])
    assert apartment['kind'] == 'apartment'
    assert apartment['latitude']
    assert apartment['longitude']
    assert apartment['manager_id']
    assert apartment['number_bathrooms']
    assert apartment['number_bedrooms']
    assert apartment['number_of_views']
    assert apartment['page_url']
    assert len(apartment['photos'])
    assert apartment['price']
    assert apartment['price_period'] == 'month'
    assert apartment['publish_date']
