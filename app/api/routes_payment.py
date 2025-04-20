from flask import Blueprint, request, jsonify
from app.services.payment_service import (
    get_all_payments,
    get_payment_by_id,
    create_payment,
    update_payment,
    delete_payment
)

payment_blueprint = Blueprint('payment', __name__)

@payment_blueprint.route('/', methods=['GET'])
def list_payments():
    return jsonify(get_all_payments())

@payment_blueprint.route('/<string:message_id>', methods=['GET'])
def get_payment(message_id):
    return jsonify(get_payment_by_id(message_id))

@payment_blueprint.route('/', methods=['POST'])
def add_payment():
    data = request.get_json()
    return jsonify(create_payment(data)), 201

@payment_blueprint.route('/<string:message_id>', methods=['PUT'])
def modify_payment(message_id):
    data = request.get_json()
    return jsonify(update_payment(message_id, data))

@payment_blueprint.route('/<string:message_id>', methods=['DELETE'])
def remove_payment(message_id):
    return jsonify(delete_payment(message_id))
