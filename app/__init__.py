from flask import Flask
from app.api.routes_auth import auth_blueprint

def create_app():
    app = Flask(__name__)
    app.register_blueprint(auth_blueprint, url_prefix='/api')
    return app