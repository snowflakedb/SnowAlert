from flask import Blueprint, jsonify, request
from functools import wraps

import json
import logbook
import importlib

from connectors import CONNECTION_OPTIONS
from runners.helpers import db, dbconfig
from runners.utils import format_exception_only

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


def jsonified(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            result.setdefault('success', True)

        except Exception as e:
            result = {
                'success': False,
                'errorMessage': format_exception_only(e),
            }

        return jsonify(result)

    return wrapper


@data_api.route('/connectors/<connector>/<name>', methods=['POST'])
@cache_oauth_connection
@jsonified
def post_connector(connector, name):
    options = request.get_json()
    connector = importlib.import_module(f"connectors.{connector}")

    for opt in connector.CONNECTION_OPTIONS:
        opt_name = opt['name']
        if opt.get('required') and opt_name not in options:
            return {
                'success': False,
                'errorMessage': f"Missing required option '{opt_name}'.",
            }

    return connector.connect(name, options)


@data_api.route('/connectors/<connector>/<name>/finalize', methods=['POST'])
@cache_oauth_connection
@jsonified
def post_connector_finalize(connector, name):
    connector = importlib.import_module(f"connectors.{connector}")
    return connector.finalize(name)


@data_api.route('/connectors/<connector>/<name>/test', methods=['POST'])
@cache_oauth_connection
@jsonified
def post_connector_test(connector, name):
    connector = importlib.import_module(f"connectors.{connector}")
    return list(connector.test(name))
