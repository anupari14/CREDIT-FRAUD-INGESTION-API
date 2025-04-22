from flask import Blueprint, request, jsonify
from app.services.dispute_service import (
    get_all_disputes,
    get_dispute_by_id,
    create_dispute,
    update_dispute,
    delete_dispute,
    create_disputes_batch
)

dispute_blueprint = Blueprint('dispute', __name__)

@dispute_blueprint.route('/', methods=['GET'])
def list_disputes():
    return jsonify(get_all_disputes())

@dispute_blueprint.route('/<string:dispute_id>', methods=['GET'])
def get_dispute(dispute_id):
    return jsonify(get_dispute_by_id(dispute_id))

@dispute_blueprint.route('/', methods=['POST'])
def add_dispute():
    data = request.get_json()
    return jsonify(create_dispute(data)), 201

@dispute_blueprint.route('/<string:dispute_id>', methods=['PUT'])
def update_dispute_route(dispute_id):
    data = request.get_json()
    return jsonify(update_dispute(dispute_id, data))

@dispute_blueprint.route('/<string:dispute_id>', methods=['DELETE'])
def delete_dispute_route(dispute_id):
    return jsonify(delete_dispute(dispute_id))

@dispute_blueprint.route('/batch', methods=['POST'])
def add_dispute_batch():
    data_list = request.get_json()
    if not isinstance(data_list, list):
        return jsonify({"error": "Expected a list of dispute entries"}), 400
    return jsonify(create_disputes_batch(data_list)), 201
