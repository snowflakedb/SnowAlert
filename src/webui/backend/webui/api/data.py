from flask import Blueprint, jsonify, request
from functools import wraps

import json
import logbook
import importlib

from connectors import CONNECTION_OPTIONS
from runners.helpers import db, dbconfig

logger = logbook.Logger(__name__)

data_api = Blueprint('data', __name__)


@data_api.route('/', methods=['GET'])
def get_data():
    return jsonify(connectors=CONNECTION_OPTIONS)


@data_api.route('/connectors', methods=['GET'])
def get_connectors():
    return jsonify(CONNECTION_OPTIONS)


def cache_oauth_connection(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        oauth = json.loads(request.headers.get('Authorization') or '{}')
        if not oauth and not dbconfig.PRIVATE_KEY:
            return jsonify(success=False, message='please log in')
        db.connect(oauth=oauth, set_cache=True)
        return f(*args, **kwargs)

    return wrapper


@data_api.route('/connectors/<connector>/<name>', methods=['POST'])
@cache_oauth_connection
@jsonify
def post_connector(connector, name):
    body = request.get_json()
    try:
        connector = importlib.import_module(f"connectors.{connector}")
        return connector.connect(name, body)

    except Exception as e:
        return {
            'newStage': 'error',
            'message': e
        }


@data_api.route('/connectors/<connector>/<name>/finalize', methods=['POST'])
@cache_oauth_connection
@jsonify
def post_connector_finalize(connector, name):
    try:
        connector = importlib.import_module(f"connectors.{connector}")
        return connector.finalize(name)

    except Exception as e:
        return {
            'newStage': 'error',
            'message': e
        }


@data_api.route('/connectors/<connector>/<name>/test', methods=['POST'])
@cache_oauth_connection
@jsonify
def post_connector_test(connector, name):
    connector = importlib.import_module(f"connectors.{connector}")
    try:
        return list(connector.test(name))

    except Exception as e:
        return {
            'newStage': 'error',
            'message': e
        }
