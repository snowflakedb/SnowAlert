import sys

from flask import Flask, request
import logbook

from samui import config
from samui.gunicorn_conf import host, port
from samui.api import rules_api
from samui.api.oauth import oauth_api
from samui.views import app_views

logger = logbook.Logger(__name__)

app = Flask(__name__.split('.')[0], static_folder=None)  # type: ignore
app.config.from_object(config.FlaskConfig)  # type: ignore
app.debug = config.DEBUG

app.register_blueprint(app_views)
app.register_blueprint(rules_api, url_prefix='/api/sa/rules')
app.register_blueprint(oauth_api, url_prefix='/api/sa/oauth')


@app.errorhandler(Exception)
def error_handler(ex):
    logger.exception('An error has occurred! ({} {} {} {})'.format(
        request.remote_addr, request.method, request.scheme, request.full_path))
    return 'Internal Server Error', 500


def main():
    logbook.StreamHandler(sys.stdout).push_application()
    app.run(host, port)


if __name__ == '__main__':
    main()
