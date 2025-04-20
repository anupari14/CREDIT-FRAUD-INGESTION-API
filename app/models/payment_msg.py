from flask_sqlalchemy import SQLAlchemy
import os

db = SQLAlchemy()

class PaymentMessage(db.Model):
    __tablename__ = "payment_msgs_raw"
    __table_args__ = {"schema": os.getenv("DB_SCHEMA", "public")}
    message_id = db.Column(db.String, primary_key=True)
    card_number_token = db.Column(db.String)
    merchant_id = db.Column(db.String)
    device_id = db.Column(db.String)
    amount = db.Column(db.Float)
    currency = db.Column(db.String)
    mcc = db.Column(db.String)
    channel = db.Column(db.String)
    country = db.Column(db.String)
    response_code = db.Column(db.String)
    auth_code = db.Column(db.String)
    iso_message_hex = db.Column(db.Text)
    gateway_provider = db.Column(db.String)
    status = db.Column(db.String)
    risk_score = db.Column(db.Float)
    ip_address = db.Column(db.String)
