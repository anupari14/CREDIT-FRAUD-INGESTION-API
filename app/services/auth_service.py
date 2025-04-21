from app.models.auth_log_msg import db, AuthLogMessage

def get_all_auth_logs():
    return [log.to_dict() for log in AuthLogMessage.query.all()]

def get_auth_log_by_id(auth_event_id):
    log = AuthLogMessage.query.get(auth_event_id)
    return log.to_dict() if log else {"error": "Not found"}

def create_auth_log(data):
    log = AuthLogMessage(**data)
    db.session.add(log)
    db.session.commit()
    return log.to_dict()

def update_auth_log(auth_event_id, data):
    log = AuthLogMessage.query.get(auth_event_id)
    if not log:
        return {"error": "Not found"}
    for key, value in data.items():
        if hasattr(log, key):
            setattr(log, key, value)
    db.session.commit()
    return log.to_dict()

def delete_auth_log(auth_event_id):
    log = AuthLogMessage.query.get(auth_event_id)
    if not log:
        return {"error": "Not found"}
    db.session.delete(log)
    db.session.commit()
    return {"message": "Deleted"}

def create_auth_logs_batch(data_list):
    logs = [AuthLogMessage(**data) for data in data_list]
    db.session.bulk_save_objects(logs)
    db.session.commit()
    return {"message": f"{len(logs)} log(s) inserted successfully"}
