import boto3
import pytest
from datetime import datetime

from unittest.mock import patch

import queues


def test_send_offers(offer, offers_queue_url):
    assert queues.send_offers([offer], offers_queue_url) == 1
