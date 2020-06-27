import requests

from unittest.mock import MagicMock
from runners.handlers import service_now


def test_handler():
    post_mock = MagicMock(return_value={'status': 'ok'})
    requests.post = post_mock

    requests.post(asdf=21)
    service_now.handle({})

    post_mock.assert_called_once()
