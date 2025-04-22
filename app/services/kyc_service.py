from app.models import db
from app.models.kyc_msg import KYCMessage

def get_all_kyc_msgs():
    return [msg.to_dict() for msg in KYCMessage.query.all()]

def get_kyc_msg_by_id(kyc_event_id):
    msg = KYCMessage.query.get(kyc_event_id)
    return msg.to_dict() if msg else {"error": "Not found"}

def create_kyc_msg(data):
    msg = KYCMessage(**data)
    db.session.add(msg)
    db.session.commit()
    return msg.to_dict()

def update_kyc_msg(kyc_event_id, data):
    msg = KYCMessage.query.get(kyc_event_id)
    if not msg:
        return {"error": "Not found"}
    for key, value in data.items():
        if hasattr(msg, key):
            setattr(msg, key, value)
    db.session.commit()
    return msg.to_dict()

def delete_kyc_msg(kyc_event_id):
    msg = KYCMessage.query.get(kyc_event_id)
    if not msg:
        return {"error": "Not found"}
    db.session.delete(msg)
    db.session.commit()
    return {"message": "Deleted"}

def create_kyc_batch(data_list):
    for row in data_list:
        if not row.get("timestamp"):
            row["timestamp"] = None
    msgs = [KYCMessage(**row) for row in data_list]
    db.session.bulk_save_objects(msgs)
    db.session.commit()
    return {"message": f"{len(msgs)} KYC records inserted successfully"}
