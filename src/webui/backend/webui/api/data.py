from flask import Blueprint, jsonify, request
from functools import wraps

import json
import logbook
import importlib

from connectors import CONNECTION_OPTIONS
from runners.helpers import db, dbconfig, vault, log
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
            log.error(e)
            result = {'success': False, 'errorMessage': format_exception_only(e)}

        return jsonify(result)

    return wrapper


@data_api.route('/connectors/<connector>/<name>', methods=['POST'])
@cache_oauth_connection
@jsonified
def post_connector(connector, name):
    options = request.get_json()
    connector = importlib.import_module(f"connectors.{connector}")

    required_options = {
        o['name']: o for o in connector.CONNECTION_OPTIONS if o.get('required')
    }
    missing_option_names = set(required_options) - set(options)
    if missing_option_names:
        missing_options = [required_options[n] for n in missing_option_names]
        missing_titles = set(o.get('title', o['name']) for o in missing_options)
        missing_titles_str = '\n  - ' + '\n  - '.join(missing_titles)
        return {
            'success': False,
            'errorMessage': f"Missing required configuration options:{missing_titles_str}",
        }

    for o in connector.CONNECTION_OPTIONS:
        oname = o['name']
        ovalue = options.get(oname)

        # lists that are passed in as strings are comma-saparated
        if o.get('type') == 'list' and type(ovalue) is str:
            options[oname] = None if ovalue is None else ovalue.split(',')

    # TODO: fix connection options to support secret ints (probs w/ one for loop)
    int_option_names = {
        o['name'] for o in connector.CONNECTION_OPTIONS if o.get('type') == 'int'
    }
    for opt_name in int_option_names:
        options[opt_name] = int(options[opt_name])

    secret_option_names = {
        o['name'] for o in connector.CONNECTION_OPTIONS if o.get('secret')
    }
    for opt_name in secret_option_names:
        if vault.ENABLED and opt_name in options:
            options[opt_name] = vault.encrypt(options[opt_name])

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
