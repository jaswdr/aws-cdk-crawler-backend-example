import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

import enrichment


def test_enrich_offer(offer):
    result_offer = enrichment.enrich_offer(offer)
    assert 'spire_distance_in_km', result_offer
    assert result_offer['spire_distance_in_km']
