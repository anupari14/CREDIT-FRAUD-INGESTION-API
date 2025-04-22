from flask import Blueprint, request, jsonify
from app.services.kyc_service import (
    get_all_kyc_msgs,
    get_kyc_msg_by_id,
    create_kyc_msg,
    update_kyc_msg,
    delete_kyc_msg,
    create_kyc_batch
)

kyc_blueprint = Blueprint('kyc', __name__)

@kyc_blueprint.route('/', methods=['GET'])
def list_kyc():
    return jsonify(get_all_kyc_msgs())

@kyc_blueprint.route('/<string:kyc_event_id>', methods=['GET'])
def get_kyc(kyc_event_id):
    return jsonify(get_kyc_msg_by_id(kyc_event_id))

@kyc_blueprint.route('/', methods=['POST'])
def add_kyc():
    return jsonify(create_kyc_msg(request.get_json())), 201

@kyc_blueprint.route('/<string:kyc_event_id>', methods=['PUT'])
def update_kyc(kyc_event_id):
    return jsonify(update_kyc_msg(kyc_event_id, request.get_json()))

@kyc_blueprint.route('/<string:kyc_event_id>', methods=['DELETE'])
def delete_kyc(kyc_event_id):
    return jsonify(delete_kyc_msg(kyc_event_id))

@kyc_blueprint.route('/batch', methods=['POST'])
def add_kyc_batch():
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({"error": "Expected a list of KYC records"}), 400
    return jsonify(create_kyc_batch(data)), 201
