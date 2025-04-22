from app.models import db
from app.models.dispute_msg import DisputeMessage

def get_all_disputes():
    return [d.to_dict() for d in DisputeMessage.query.all()]

def get_dispute_by_id(dispute_id):
    dispute = DisputeMessage.query.get(dispute_id)
    return dispute.to_dict() if dispute else {"error": "Not found"}

def create_dispute(data):
    dispute = DisputeMessage(**data)
    db.session.add(dispute)
    db.session.commit()
    return dispute.to_dict()

def update_dispute(dispute_id, data):
    dispute = DisputeMessage.query.get(dispute_id)
    if not dispute:
        return {"error": "Not found"}
    for key, value in data.items():
        if hasattr(dispute, key):
            setattr(dispute, key, value)
    db.session.commit()
    return dispute.to_dict()

def delete_dispute(dispute_id):
    dispute = DisputeMessage.query.get(dispute_id)
    if not dispute:
        return {"error": "Not found"}
    db.session.delete(dispute)
    db.session.commit()
    return {"message": "Deleted"}

def create_disputes_batch(data_list):
    disputes = [DisputeMessage(**data) for data in data_list]
    db.session.bulk_save_objects(disputes)
    db.session.commit()
    return {"message": f"{len(disputes)} dispute(s) inserted successfully"}
