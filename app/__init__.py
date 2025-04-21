from flask import Flask
from app.config.settings import get_config
from app.models.payment_msg import db
from app.api.routes_auth import auth_blueprint
from app.api.routes_payment import payment_blueprint
from app.api.routes_auth_log import authlog_blueprint

def create_app():
    app = Flask(__name__)
    app.config.from_object(get_config())
    db.init_app(app)
    app.register_blueprint(auth_blueprint, url_prefix='/api/auth')
    app.register_blueprint(payment_blueprint, url_prefix='/api/payment')
    app.register_blueprint(authlog_blueprint, url_prefix='/api/authlog')
    return app