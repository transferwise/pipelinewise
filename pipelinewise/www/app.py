import secrets
import logging

from flask import Flask
from flask import Blueprint

LOGGER = logging.getLogger(__name__)


class PipelinewiseApp:
    app = Flask('Pipelinewise')
    api = Blueprint('api', __name__)
    api_url = '/api/v1'

    def __init__(
        self,
        app_name='Pipelinewise',
        host=None,
        port=None,
        backend_db=None,
    ):
        self.app.secret_key = secrets.token_hex()
        self.app.config['APP_NAME'] = app_name
        self.host = host
        self.port = port
        self.backend_db = backend_db

        # Register API endpoints
        self.app.register_blueprint(PipelinewiseApp.api, url_prefix=PipelinewiseApp.api_url)

    def run(self):
        self.app.run(
            debug=True,
            use_reloader=True,
            host='pipelinewise_dev',
            port=self.port,
        )

    @api.route('/health')
    def health():
        return {'status': 'ok'}

    @api.route('/import', methods=['POST'])
    def import_project():
        return {
            'warning': 'Import request received but functionality not implemented yet'
        }
