import boto3
import logging
import json
from datetime import datetime, timedelta

from utils import chunks, offer_id
from config import logging_level

log = logging.getLogger(__name__)
log.setLevel(logging_level())

MAX_QUEUE_CHUNK_SIZE = 10


def _get_sqs_client():
    return boto3.client('sqs')


def _entries_from_offers(offers):
    entries = []
    for offer in offers:
        _id = offer_id(offer)
        entries.append({
            'Id': _id,
            'MessageBody': json.dumps(offer)})
    return entries


def send_offers(offers, queue_url):
    log.debug(f"Sending {len(offers)} offers")
    client = _get_sqs_client()
    total_sent = 0
    for chunk in chunks(offers, MAX_QUEUE_CHUNK_SIZE):
        entries = _entries_from_offers(chunk)
        response = client.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            log.error(response)
            raise RuntimeError(response)
        total_sent += len(chunk)
    return total_sent
