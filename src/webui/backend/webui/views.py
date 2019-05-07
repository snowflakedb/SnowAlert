import logbook
from flask import Blueprint, send_from_directory, send_file

from . import config

app_views = Blueprint('app_views', __name__)

logger = logbook.Logger(__name__)


@app_views.route('/')
def index():
    return send_file(str(config.BUILD_FOLDER.joinpath('index.html')))


@app_views.route('/<path:path>')
def serve_resources(path):
    try:
        return send_from_directory(config.STATIC_FOLDER, path)
    except Exception:
        logger.error(f'Bad resource access: {path}')
        return index()
