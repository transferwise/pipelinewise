#!/usr/bin/env python3

import os

from flask import Flask, Response, url_for, jsonify, request
from flask_cors import CORS
from rest_api.manager import Manager
from rest_api.auth import requires_auth

app = Flask(__name__)
CORS(app)

config_dir = os.path.join(os.getcwd(), '../config')
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
    app.run()

if __name__ == '__main__':
    main()