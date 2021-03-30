import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

import queues


def test__get_sqs_client():
    client = queues._get_sqs_client()
    assert client.__class__.__name__ == 'SQS'


def test__entries_from_offers(offer):
    entries = queues._entries_from_offers([offer])
    assert len(entries) == 1
    entry = entries.pop()
    assert entry['Id']
    assert entry['MessageBody']


def test_send_offers(offer):
    fake_sqs_client = Mock()
    fake_sqs_client.send_message_batch.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

    with patch('queues._get_sqs_client', lambda: fake_sqs_client):
        queues.send_offers([offer], 'TestQueueUrl')
    fake_sqs_client.send_message_batch.assert_called_with(
            QueueUrl='TestQueueUrl',
            Entries=[{
                    'Id': 'http-localhost',
                    'MessageBody': json.dumps(offer)}])
