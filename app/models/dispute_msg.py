import os
from app.models import db

class DisputeMessage(db.Model):
    __tablename__ = "dispute_msgs_raw"
    __table_args__ = {"schema": os.getenv("DB_SCHEMA", "as-is-data-schema")}

    dispute_id = db.Column(db.String, primary_key=True)
    transaction_id = db.Column(db.String, nullable=False)
    customer_id = db.Column(db.String, nullable=False)
    merchant_id = db.Column(db.String, nullable=False)
    amount = db.Column(db.Float)
    currency = db.Column(db.String)
    timestamp = db.Column(db.DateTime, nullable=False)
    dispute_reason_code = db.Column(db.String)
    dispute_stage = db.Column(db.String)
    status = db.Column(db.String)
    evidence_provided = db.Column(db.Boolean)
    resolution_timestamp = db.Column(db.DateTime)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
