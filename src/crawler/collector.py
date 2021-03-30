from datetime import datetime
from requests.exceptions import ConnectionError
from retrying import retry
from urllib.parse import urljoin
from urllib.parse import urlparse, parse_qs
import logging
import mechanicalsoup
import re


from config import logging_level
from operations import KIND_APARTMENT
from user_agents import get_user_agent

log = logging.getLogger(__name__)
log.setLevel(logging_level())

BASE_URL = 'https://www.daft.ie'
APARTMENTS_SEARCH_URL = '/property-for-rent/dublin/apartments?pageSize=20&from={}'


def retrying_on_connection_error(exception):
    log.debug('Retrying due to connection error')
    return isinstance(exception, ConnectionError)


@retry(wait_fixed=3000, retry_on_exception=retrying_on_connection_error,
       stop_max_attempt_number=7)
def get_browser(url):
    log.debug('Entering {}'.format(url))

    user_agent = get_user_agent()
    log.debug(f'Using {user_agent} user agent')

    headers = {'Accept-Encoding': 'gzip, compress, br'}

    browser = mechanicalsoup.StatefulBrowser(user_agent=user_agent)
    browser.open(url,
                 headers=headers,
                 verify=False)
    return browser


def collect_detail_page(detail_browser):
    # number_bathrooms
    number_bathrooms = detail_browser.page.select_one(
        'p.TitleBlock__CardInfoItem-sc-1avkvav-8:nth-child(2)').text
    number_bathrooms = int(number_bathrooms[0])

    # number_bedrooms
    number_bedrooms = detail_browser.page.select_one(
        'p.TitleBlock__CardInfoItem-sc-1avkvav-8:nth-child(1)').text
    number_bedrooms = int(number_bedrooms[0])

    # price and price_period
    price_raw = detail_browser.page.select_one(
        '.TitleBlock__StyledSpan-sc-1avkvav-4').text.split()
    price = int(''.join([f for f in price_raw[0] if f.isdigit()]))
    price_period = price_raw[2].lower()

    # number_of_views
    number_of_views = detail_browser.page.select_one(
        'div.Statistics__StyledStatsContainer-sc-15tgae4-0:nth-child(2) > div:nth-child(1) > p:nth-child(1)').text
    number_of_views = int(
        ''.join([f for f in number_of_views if f.isdigit()]))

    # photos
    photos = [f.attrs['src'] if f else None for f in [
        detail_browser.page.select_one(
            'img[data-testid="main-header-image"]'),
        detail_browser.page.select_one(
            'img[data-testid="extra-header-image-0"]'),
        detail_browser.page.select_one(
            'img[data-testid="extra-header-image-1"]'),
        detail_browser.page.select_one(
            'img[data-testid="extra-header-image-2"]'),
    ]]
    photos = list(filter(lambda x: x, photos))

    # getting latitude and longitude
    latitude = 0.0
    longitude = 0.0
    google_maps_url = [
        f.attrs.get('href', '') for f in detail_browser.page.select('a')
        if 'viewpoint' in f.attrs.get('href', '')].pop()
    if google_maps_url:
        url = urlparse(google_maps_url)
        qs = parse_qs(url.query)
        viewpoint = qs['viewpoint'].pop()
        lat_str, long_str = viewpoint.split(',')
        latitude = float(lat_str)
        longitude = float(long_str)

    return {
        'kind': KIND_APARTMENT,
        'page_url': detail_browser.url,
        'number_bathrooms': number_bathrooms,
        'number_bedrooms': number_bedrooms,
        'description': detail_browser.page.select_one('.PropertyPage__StandardParagraph-sc-14jmnho-8').text,
        'display_address': detail_browser.page.select_one('.TitleBlock__Address-sc-1avkvav-7').text,
        'features': [
            f.text for f in detail_browser.page.select('li.PropertyDetailsList__PropertyDetailsListItem-sc-1cjwtjz-1')],
        'manager_id': detail_browser.page.select_one('.ContactPanel__ImageLabel-sc-18zt6u1-6').text,
        'photos': photos,
        'price': price,
        'price_period': price_period,
        'number_of_views': number_of_views,
        'latitude': latitude,
        'longitude': longitude,
        'publish_date': detail_browser.page.select_one('div.Statistics__StyledStatsContainer-sc-15tgae4-0:nth-child(1) > div:nth-child(1) > p:nth-child(1)').text,
        'collected_at': datetime.utcnow().timestamp()}


def collect_apartments_page(current_page, limit=0):
    apartments = list()
    try:
        browser = get_browser(
            urljoin(
                BASE_URL,
                APARTMENTS_SEARCH_URL.format(
                    current_page * 20)))
    except ConnectionError as err:
        log.error(err)
        return apartments

    links = browser.links(url_regex=r'\/for\-rent\/apartment\-\S+')
    processed_links = []
    browser.close()
    for link in links:
        href = urljoin(BASE_URL, link['href'])
        try:
            detail_browser = get_browser(href)
        except ConnectionError as err:
            log.error(err)
            return apartments

        # Avoid process the same URL again
        if detail_browser.url in processed_links:
            continue
        processed_links.append(detail_browser.url)

        # Ignoring when we are redirected because the Ad has expired
        if 'expired' in detail_browser.url:
            log.debug(f'Ignoring expired {detail_browser.url}')
            continue

        new_apartment = collect_detail_page(detail_browser)
        detail_browser.close()
        apartments.append(new_apartment)
        if limit:
            if len(apartments) >= limit:
                return apartments

    return apartments
