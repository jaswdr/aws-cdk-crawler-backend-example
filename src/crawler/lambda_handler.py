import json
import logging

from collector import collect_apartments_page
from operations import save_offers_to_ddb_table, save_enriched_offers_to_ddb_table
from enrichment import enrich_offer
from queues import send_offers
from config import logging_level, offers_table_name, offers_queue_url
from search import search_offers

log = logging.getLogger(__name__)
log.setLevel(logging_level())


def handler(event, context):
    return {'fn': 'default_handler', 'event': event}


def crawler(event, context):
    log.debug(f'Received new event: {event}')
    log.debug('Collecting first page')
    offers = collect_apartments_page(0)
    log.debug(f'Collected {len(offers)} offers')

    table_name = offers_table_name()
    log.debug(f'Saving to DynamoDB: {table_name}')
    save_offers_to_ddb_table(offers, table_name)
    log.debug(f'Saved to DynamoDB table: {table_name}')

    queue_url = offers_queue_url()
    log.debug(f'Sending to SQS queue: {queue_url}')
    send_offers(offers, queue_url)
    log.debug(f'Sent to SQS queue: {queue_url}')

    return {'status_code': 200, 'total_collected': len(offers)}


def enrichment(event, context):
    log.debug(f'Received new event: {event}')

    records = event['Records']
    log.debug(f'Enriching offers: {len(records)}')
    enriched_offers = []
    for record in event['Records']:
        offer = json.loads(record['body'])
        enriched_offer = enrich_offer(offer)
        enriched_offers.append(enriched_offer)

    log.debug(f'Saving enriched offers: {len(enriched_offers)}')
    table_name = offers_table_name()
    save_enriched_offers_to_ddb_table(enriched_offers, table_name)

    return {'status_code': 200, 'total_enriched': len(enriched_offers)}


def search(event, context):
    log.debug(f'Received new event: {event}')

    query = event['rawQueryString']
    log.debug(f'Searching offers: {query}')
    table_name = offers_table_name()
    offers = search_offers(query, table_name)

    log.debug(f'Found {len(offers)} offers')

    return {'status_code': 200, 'offers': offers}
