import decimal
import orjson
import re


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def offer_id(offer):
    page_url = offer['page_url']
    if len(page_url) > 80:
        page_url = page_url[80:]
    return re.sub(r'\W+', '-', page_url)


def boto_status_code(response):
    return response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)


def object_to_dict(obj):
    def default(val):
        if isinstance(val, decimal.Decimal):
            return str(val)
        raise TypeError

    return orjson.loads(orjson.dumps(obj, default=default))
