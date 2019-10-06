import sys

from flask import Flask, request, json
import logbook

from webui import config
from webui.gunicorn_conf import host, port
from webui.api import rules_api, data_api
from webui.api.oauth import oauth_api
from webui.views import app_views

from runners.utils import json_dumps


URL_EXTENSIONS_CACHED = ('js', 'woff2', 'css')


class SAJSONEncoder(json.JSONEncoder):
    def default(self, o):
        return json_dumps(o)


class SAFlask(Flask):
    def json_encoder(self, **kwargs):
        return SAJSONEncoder(**kwargs)

    def get_send_file_max_age(self, name):
        if not name.split('.')[-1].lower() in URL_EXTENSIONS_CACHED:
            return 0
        return Flask.get_send_file_max_age(self, name)


logger = logbook.Logger(__name__)

app = SAFlask(__name__.split('.')[0], static_folder=None)  # type: ignore
app.config.from_object(config.FlaskConfig)  # type: ignore
app.debug = config.DEBUG

app.register_blueprint(app_views)
app.register_blueprint(data_api, url_prefix='/api/sa/data')
app.register_blueprint(rules_api, url_prefix='/api/sa/rules')
app.register_blueprint(oauth_api, url_prefix='/api/sa/oauth')


@app.errorhandler(Exception)
def error_handler(ex):
    logger.exception(
        'An error has occurred! ({} {} {} {})'.format(
            request.remote_addr, request.method, request.scheme, request.full_path
        )
    )
    return 'Internal Server Error', 500


def main():
    logbook.StreamHandler(sys.stdout).push_application()
    app.run(host, port)


if __name__ == '__main__':
    main()
