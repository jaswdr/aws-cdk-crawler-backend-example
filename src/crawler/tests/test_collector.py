import os
from unittest.mock import Mock, patch

from requests.exceptions import ConnectionError
from requests import Response
import mechanicalsoup

from collector import *


def test_get_user_agent_returns_non_empty_user_agent():
    assert get_user_agent() != ''


def test_retrying_on_connection_error_is_true_for_connection_error():
    assert retrying_on_connection_error(ConnectionError('')) is True


def test_retrying_on_connection_error_is_false_for_other_exceptions():
    assert retrying_on_connection_error(Exception('')) is False


def test_collect_page_returns_expected_values(monkeypatch):
    fake_bathrooms = Mock()
    fake_bathrooms.text = '1 Bath'

    fake_bedrooms = Mock()
    fake_bedrooms.text = '2 Bed'

    fake_price = Mock()
    fake_price.text = '€2,300 per month'

    fake_number_of_views = Mock()
    fake_number_of_views.text = '6,925'

    fake_description = Mock()
    fake_description.text = 'Test Description'

    fake_display_address = Mock()
    fake_display_address.text = 'Test Address'

    fake_feature = Mock()
    fake_feature.text = 'Parking'

    fake_manager_id = Mock()
    fake_manager_id.text = 'Laura Doe'

    fake_photo = Mock()
    fake_photo.attrs = {'src': 'http://localhost'}

    fake_created_at = Mock()
    fake_created_at.text = '03/03/2021'

    fake_maps = Mock()
    fake_maps.attrs = {'href': 'https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=53.344905963613485,-6.231118982370589'}

    fake_page = Mock()
    fake_page.select_one.side_effect = [
        fake_bathrooms,
        fake_bedrooms,
        fake_price,
        fake_number_of_views,
        fake_photo,
        fake_photo,
        fake_photo,
        fake_photo,
        fake_description,
        fake_display_address,
        fake_manager_id,
        fake_created_at]

    fake_page.select.side_effect = [
            [fake_maps],
            [fake_feature, fake_feature]]

    fake_browser = Mock()
    fake_browser.links.return_value = [{'href': 'http://localhost'}]
    fake_browser.page = fake_page
    fake_browser.url = 'http://localhost'

    with patch('collector.get_browser', lambda _: fake_browser):
        data = collect_apartments_page(0)
        assert len(data) == 1
        record = data[0]
        assert record['page_url'] == 'http://localhost'
        assert record['number_bathrooms'] == 1
        assert record['number_bedrooms'] == 2
        assert record['description'] == 'Test Description'
        assert record['display_address'] == 'Test Address'
        assert record['features'] == ['Parking', 'Parking']
        assert record['manager_id'] == 'Laura Doe'
        assert record['photos'] == [
            'http://localhost',
            'http://localhost',
            'http://localhost',
            'http://localhost']
        assert record['price'] == 2300
        assert record['price_period'] == 'month'
        assert record['number_of_views'] == 6925
        assert record['publish_date'] == '03/03/2021'
        assert record['latitude'] == 53.344905963613485
        assert record['longitude'] == -6.231118982370589
