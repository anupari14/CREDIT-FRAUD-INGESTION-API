import os
from app.models import db

class KYCMessage(db.Model):
    __tablename__ = "kyc_msgs_raw"
    __table_args__ = {"schema": os.getenv("DB_SCHEMA", "as-is-data-schema")}

    kyc_event_id = db.Column(db.String, primary_key=True)
    customer_id = db.Column(db.String, nullable=False)
    device_id = db.Column(db.String, nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False)
    kyc_type = db.Column(db.String)
    document_type = db.Column(db.String)
    document_number_hash = db.Column(db.String)
    face_match_score = db.Column(db.Float)
    verification_status = db.Column(db.String)
    geo_location = db.Column(db.String)
    ip_address = db.Column(db.String)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
