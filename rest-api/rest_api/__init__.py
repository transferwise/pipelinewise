#!/usr/bin/env python3

import os
from rest_api.settings import *

from pathlib import Path

from flask import Flask, Response, url_for, jsonify, request
from flask_cors import CORS
from rest_api.manager import Manager
from rest_api.auth import requires_auth

app = Flask(__name__)

# There are three pre-defined settings:
# Development, Testing and Default (Production)
config = {
    'development': 'rest_api.DevelopmentSettings',
    'testing': 'rest_api.TestingSettings',
    'default': 'rest_api.ProductionSettings'
}

# Load one of the pre-defined settings
# The desired settings needs to be defined in the PIPELINEWISW_SETTINGS environment variable
config_name = os.getenv('PIPELINEWISE_SETTINGS', 'default')
app.config.from_object(config[config_name])

# Override any pre-defined settings from an external file
# The file path needs to be defined in the PIPELINEWISE_SETTINGS_FILE environment variable
app.config.from_envvar('PIPELINEWISE_SETTINGS_FILE', silent=True)
CORS(app)


config_dir = os.path.join(Path.home(), '.pipelinewise')
venv_dir = os.path.join(os.getcwd(), '../.virtualenvs')
manager = Manager(config_dir, venv_dir, app.logger)

class WebadminException(Exception):
    '''A known exception for which we don't need to pring a stack trace'''
    pass

@app.errorhandler(Exception)
def all_exception_handler(error):
    resp = jsonify({
        'status': 500,
        'message': 'Backend Error - {}'.format(error)
    })
    resp.status_code = 200
    return resp

@app.errorhandler(404)
def not_found(error=None):
    resp = jsonify({
        'status': 404,
        'message': 'Unknown API URL: ' + request.url
    })
    resp.status_code = 404
    return resp

@app.route("/", methods = ['GET'])
def hello():
    return send_from_directory('static')

@app.route("/secrets")
@requires_auth
def api_hello():
    return "Shhh this is top secret"

@app.route("/config", methods = ['GET'])
def api_config():
    return jsonify({
        'status': 200,
        'result': manager.get_config()
    })

@app.route("/add", methods = ["POST"])
def api_add_target():
    return jsonify({
        'status': 200,
        'result': manager.add_target(request.get_json())
    })

@app.route("/targets", methods = ['GET'])
def api_get_targets():
    return jsonify({
        'status': 200,
        'result': manager.get_targets()
    })

@app.route("/targets/<target_id>", methods = ['GET'])
def api_get_target(target_id):
    return jsonify({
        'status': 200,
        'result': manager.get_target(target_id)
    })

@app.route("/targets/<target_id>/delete", methods = ['DELETE'])
def api_delete_target(target_id):
    return jsonify({
        'status': 200,
        'result': manager.delete_target(target_id)
    })

@app.route("/targets/<target_id>/config", methods = ['GET'])
def api_get_target_config(target_id):
    return jsonify({
        'status': 200,
        'result': manager.get_target_config(target_id)
    })

@app.route("/targets/<target_id>/config", methods = ['POST'])
def api_update_target_config(target_id):
    return jsonify({
        'status': 200,
        'result': manager.update_target_config(target_id, request.get_json())
    })

@app.route("/targets/<target_id>/add", methods = ["POST"])
def api_add_tap(target_id):
    return jsonify({
        'status': 200,
        'result': manager.add_tap(target_id, request.get_json())
    })

@app.route("/targets/<target_id>/taps", methods = ['GET'])
def api_get_taps(target_id):
    return jsonify({
        'status': 200,
        'result': manager.get_taps(target_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>", methods = ['GET'])
def api_get_tap(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.get_tap(target_id, tap_id)
    })


@app.route("/targets/<target_id>/taps/<tap_id>", methods = ['PATCH'])
def api_update_tap(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.update_tap(target_id, tap_id, request.get_json())
    })

@app.route("/targets/<target_id>/taps/<tap_id>/discover", methods = ['POST'])
def api_discover_tap(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.discover_tap(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/run", methods = ['POST'])
def api_run_tap(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.run_tap(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/delete", methods = ['DELETE'])
def api_delete_tap(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.delete_tap(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/config", methods = ['GET'])
def api_get_tap_config(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.get_tap_config(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/config", methods = ['POST'])
def api_update_tap_config(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.update_tap_config(target_id, tap_id, request.get_json())
    })

@app.route("/targets/<target_id>/taps/<tap_id>/inheritableconfig", methods = ['GET'])
def api_get_inheritable_tap_config(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.get_tap_inheritable_config(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/inheritableconfig", methods = ['POST'])
def api_update_inheritable_tap_config(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.update_tap_inheritable_config(target_id, tap_id, request.get_json())
    })

@app.route("/targets/<target_id>/taps/<tap_id>/testconnection", methods = ['GET'])
def api_test_tap_connection(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.test_tap_connection(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/streams", methods = ['GET'])
def api_get_streams(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.get_streams(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/streams/<stream_id>", methods = ['GET'])
def api_get_stream(target_id, tap_id, stream_id):
    return jsonify({
        'status': 200,
        'result': manager.get_stream(target_id, tap_id, stream_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/streams/<stream_id>", methods = ['PATCH'])
def api_update_stream(target_id, tap_id, stream_id):
    return jsonify({
        'status': 200,
        'result': manager.update_stream(target_id, tap_id, stream_id, request.get_json())
    })

@app.route("/targets/<target_id>/taps/<tap_id>/transformations/<stream>", methods = ['GET'])
def api_get_transformations(target_id, tap_id, stream):
    return jsonify({
        'status': 200,
        'result': manager.get_transformations(target_id, tap_id, stream)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/transformations/<stream>/<field_id>", methods = ['PATCH'])
def api_update_transformation(target_id, tap_id, stream, field_id):
    return jsonify({
        'status': 200,
        'result': manager.update_transformation(target_id, tap_id, stream, field_id, request.get_json())
    })

@app.route("/targets/<target_id>/taps/<tap_id>/logs", methods = ['GET'])
def api_get_tap_logs(target_id, tap_id):
    return jsonify({
        'status': 200,
        'result': manager.get_tap_logs(target_id, tap_id)
    })

@app.route("/targets/<target_id>/taps/<tap_id>/logs/<log_id>", methods =['GET'])
def api_get_tap_log(target_id, tap_id, log_id):
    return jsonify({
        'status': 200,
        'result': manager.get_tap_log(target_id, tap_id, log_id)
    })

def main():
    '''Main entry point'''
    app.run(host = app.config["HOST"], port = app.config["PORT"])

if __name__ == '__main__':
    main()