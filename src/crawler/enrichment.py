from datetime import datetime
import logging

from cachetools import cached
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

from user_agents import get_user_agent
from config import logging_level


log = logging.getLogger(__name__)
log.setLevel(logging_level())


def enrich_offer(offer):
    offer = _add_points_of_interest(offer)
    return offer


def _add_points_of_interest(offer):
    offer = _add_spire_distance(offer)
    return offer


def _spire_point():
    return (53.3524573, -6.261041)


def _add_spire_distance(offer):
    offer_point = (offer['latitude'], offer['longitude'])
    distance = geodesic(offer_point, _spire_point())
    distance = round(distance.kilometers, 2)
    offer['spire_distance_in_km'] = distance
    return offer
