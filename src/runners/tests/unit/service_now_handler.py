import requests

from unittest.mock import MagicMock
from runners.handlers import service_now


def test_handler_success_created_201():
    returned_mock = MagicMock(status_code=201)
    post_mock = MagicMock(return_value=returned_mock)
    backup_post = requests.post
    requests.post = post_mock

    try:
        sn_handle_return_value = service_now.handle({})
    finally:
        requests.post = backup_post

    post_mock.assert_called_once()
    assert sn_handle_return_value is returned_mock
