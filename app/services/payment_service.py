from app.models.payment_msg import db, PaymentMessage

def get_all_payments():
    return [p.__dict__ for p in PaymentMessage.query.all()]

def get_payment_by_id(message_id):
    payment = PaymentMessage.query.get(message_id)
    return payment.__dict__ if payment else {"error": "Not found"}

def create_payment(data):
    payment = PaymentMessage(**data)
    db.session.add(payment)
    db.session.commit()
    return payment.__dict__

def update_payment(message_id, data):
    payment = PaymentMessage.query.get(message_id)
    if not payment:
        return {"error": "Not found"}
    for key, value in data.items():
        setattr(payment, key, value)
    db.session.commit()
    return payment.__dict__

def delete_payment(message_id):
    payment = PaymentMessage.query.get(message_id)
    if not payment:
        return {"error": "Not found"}
    db.session.delete(payment)
    db.session.commit()
    return {"message": "Deleted"}
