import os
from app.models import db  # <- get shared db instance


class AuthLogMessage(db.Model):
    __tablename__ = "auth_log_msgs_raw"
    __table_args__ = {"schema": os.getenv("DB_SCHEMA", "as-is-data-schema")}

    auth_event_id = db.Column(db.String, primary_key=True)
    customer_id = db.Column(db.String, nullable=False)
    device_id = db.Column(db.String, nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False)
    auth_type = db.Column(db.String)
    ip_address = db.Column(db.String)
    channel = db.Column(db.String)
    location = db.Column(db.String)
    auth_status = db.Column(db.String)
    failure_reason = db.Column(db.String)
    login_attempts = db.Column(db.Integer)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
