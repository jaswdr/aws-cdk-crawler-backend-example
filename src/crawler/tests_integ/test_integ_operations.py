import pytest
from datetime import datetime

from unittest.mock import patch

import operations

def test_save_offers_to_ddb_table(offer, offers_table):
    assert operations.save_offers_to_ddb_table([offer], offers_table) == 1


def test_save_enriched_offers_to_ddb_table(offer, offers_table):
    offer['spire_distance_in_km'] = 1.0
    assert operations.save_enriched_offers_to_ddb_table([offer], offers_table) == 1


def test_query_table(offer, offers_table):
    response = operations.query_table(offer, offers_table)
    assert response
    assert len(response)
    for item in response:
        assert item['number_bedrooms'] == offer['number_bedrooms']
        assert item['number_bathrooms'] == offer['number_bathrooms']
