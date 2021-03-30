import boto3
import logging
from datetime import datetime, timedelta

from utils import chunks, boto_status_code
from config import logging_level

KIND_APARTMENT = 'apartment'
MAX_DDB_CHUNK_SIZE = 25
LIMIT_DDB_QUERY = 10

log = logging.getLogger(__name__)
log.setLevel(logging_level())


def _get_ddb_client():
    return boto3.client('dynamodb')


def _get_ddb_resource():
    return boto3.resource('dynamodb')


def _get_table(table_name):
    return _get_ddb_resource().Table(table_name)

def _pk_appartment():
    return f"offer#{KIND_APARTMENT}"


def _pk(offer):
    return f"offer#{offer['kind']}"


def _slug(text):
    return text.lower().replace(' ', '-')


def _sk(offer):
    bedrooms = str(offer['number_bedrooms'])
    bathrooms = str(offer['number_bathrooms'])
    price = str(offer['price'])
    address = _slug(offer['display_address'])
    return '#'.join([bedrooms, bathrooms, price, address])


def _ttl():
    ttl = datetime.now()
    ttl += timedelta(days=14)
    ttl = ttl.timestamp()
    ttl = int(ttl)
    return str(ttl)


def _put_requests_from_offers(offers):
    result = []
    for offer in offers:
        item = {
            'PutRequest': {
                'Item': {
                    'PK': {
                        'S': _pk(offer)
                    },
                    'SK': {
                        'S': _sk(offer)
                    },
                    'TTL': {
                        'N': _ttl()
                    },
                    'kind': {
                        'S': offer['kind']
                    },
                    'page_url': {
                        'S': offer['page_url']
                    },
                    'number_bathrooms': {
                        'N': str(offer['number_bathrooms'])
                    },
                    'number_bedrooms': {
                        'N': str(offer['number_bedrooms'])
                    },
                    'description': {
                        'S': offer['description']
                    },
                    'display_address': {
                        'S': offer['display_address']
                    },
                    'features': {
                        'SS': offer['features']
                    },
                    'manager_id': {
                        'S': offer['manager_id']
                    },
                    'photos': {
                        'SS': offer['photos']
                    },
                    'price': {
                        'N': str(offer['price'])
                    },
                    'price_period': {
                        'S': offer['price_period']
                    },
                    'number_of_views': {
                        'N': str(offer['number_of_views'])
                    },
                    'publish_date': {
                        'S': offer['publish_date']
                    },
                    'latitude': {
                        'N': str(offer['latitude'])
                    },
                    'longitude': {
                        'N': str(offer['longitude'])
                    },
                    'collected_at': {
                        'N': str(offer['collected_at'])
                    },
                    'updated_at': {
                        'N': str(datetime.utcnow().timestamp())
                    },
                }
            }
        }
        result.append(item)
    return result


def save_offers_to_ddb_table(offers, table_name):
    log.debug(f'Saving {offers}')
    client = _get_ddb_client()
    total_saved = 0
    for chunk in chunks(offers, MAX_DDB_CHUNK_SIZE):
        response = client.batch_write_item(
            RequestItems={
                table_name: _put_requests_from_offers(chunk)})
        total_saved += len(chunk)
        log.debug(f'DynamoDB response: {response}')
    return total_saved


def _put_enrichment_requests_from_offers(offers):
    result = []
    for offer in offers:
        item = {
            'PutRequest': {
                'Item': {
                    'PK': {
                        'S': _pk(offer)
                    },
                    'SK': {
                        'S': _sk(offer)
                    },
                    'spire_distance_in_km': {
                        'N': str(offer['spire_distance_in_km'])
                    },
                }
            }
        }
        result.append(item)
    return result


def save_enriched_offers_to_ddb_table(offers, table_name):
    log.debug(f'Saving {offers}')
    client = _get_ddb_client()
    total_saved = 0
    for offer in offers:
        response = client.update_item(
            TableName=table_name,
            Key={
                'PK': {'S': _pk(offer)},
                'SK': {'S': _sk(offer)},
            },
            AttributeUpdates={
                'spire_distance_in_km': {
                    'Value': {
                        'N': str(offer['spire_distance_in_km'])
                    },
                    'Action': 'PUT'}})
        total_saved += 1
        log.debug(f'DynamoDB response: {response}')
    return total_saved


def _user_query_sk(user_query):
    number_bedrooms = user_query['number_bedrooms'] or '1'
    number_bathrooms = user_query['number_bathrooms'] or '1'
    return f'{number_bedrooms}#{number_bathrooms}'


def _user_query_to_key_conditions(user_query):
    return {
            'PK': {
                'AttributeValueList': [_pk_appartment()],
                'ComparisonOperator': 'EQ'},
            'SK': {
                'AttributeValueList': [_user_query_sk(user_query)],
                'ComparisonOperator': 'BEGINS_WITH'}}


def query_table(user_query, table_name):
    table = _get_table(table_name)
    response = table.query(
            TableName=table_name,
            Select='SPECIFIC_ATTRIBUTES',
            Limit=LIMIT_DDB_QUERY,
            ConsistentRead=False,
            AttributesToGet=[
                'page_url',
                'number_bedrooms',
                'number_bathrooms',
                'display_address',
                'price',
                'price_period',
                'publish_date'],
            KeyConditions=_user_query_to_key_conditions(user_query))
    assert boto_status_code(response) == 200
    return response['Items']
