from flask import Blueprint, request, jsonify
from app.services.auth_service import (
    get_all_auth_logs,
    get_auth_log_by_id,
    create_auth_log,
    update_auth_log,
    delete_auth_log,
    create_auth_logs_batch
)

authlog_blueprint = Blueprint('authlog', __name__)

@authlog_blueprint.route('/', methods=['GET'])
def list_logs():
    return jsonify(get_all_auth_logs())

@authlog_blueprint.route('/<string:auth_event_id>', methods=['GET'])
def get_log(auth_event_id):
    return jsonify(get_auth_log_by_id(auth_event_id))

@authlog_blueprint.route('/', methods=['POST'])
def add_log():
    data = request.get_json()
    return jsonify(create_auth_log(data)), 201

@authlog_blueprint.route('/<string:auth_event_id>', methods=['PUT'])
def update_log(auth_event_id):
    data = request.get_json()
    return jsonify(update_auth_log(auth_event_id, data))

@authlog_blueprint.route('/<string:auth_event_id>', methods=['DELETE'])
def delete_log(auth_event_id):
    return jsonify(delete_auth_log(auth_event_id))

@authlog_blueprint.route('/batch', methods=['POST'])
def add_logs_batch():
    data_list = request.get_json()
    if not isinstance(data_list, list):
        return jsonify({"error": "Expected a list of log entries"}), 400
    return jsonify(create_auth_logs_batch(data_list)), 201

