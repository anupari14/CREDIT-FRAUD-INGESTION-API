from flask import Blueprint, jsonify

auth_blueprint = Blueprint('auth', __name__)

@auth_blueprint.route('/ping', methods=['GET'])
def ping():
    return jsonify({'message': 'Auth service is up'}), 200