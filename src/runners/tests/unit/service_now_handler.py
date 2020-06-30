import requests
import pytest
from os import environ

from unittest.mock import MagicMock
from runners.handlers import service_now


@pytest.fixture
def with_oauth_envar():
    cid = environ.get('SA_SN_OAUTH_CLIENT_ID')
    if not cid:
        environ['SA_SN_OAUTH_CLIENT_ID'] = 'clientid'
    yield
    if not cid and 'SA_SN_OAUTH_CLIENT_ID' in environ:
        del environ['SA_SN_OAUTH_CLIENT_ID']


def test_handler_simpleauth():
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


def test_handler_oauth(with_oauth_envar):
    oauth_response = MagicMock(json=MagicMock(return_value={'access_token': '123'}))
    oauth_post = MagicMock(status_code=201, return_value=oauth_response)

    create_incident_post = MagicMock(
        status_code=201, json=MagicMock(return_value={'result': {'title': 'abc'}})
    )
    post_mock = MagicMock(side_effect=[oauth_post, create_incident_post])
    backup_post = requests.post
    requests.post = post_mock

    try:
        sn_handle_return_value = service_now.handle({'TITLE': 'abc'})
    finally:
        requests.post = backup_post

    assert post_mock.call_count == 2
    assert sn_handle_return_value is create_incident_post
