from app.models.payment_msg import db, PaymentMessage

def get_all_payments():
    payments = PaymentMessage.query.all()
    return [p.to_dict() for p in payments]

def get_payment_by_id(message_id):
    payment = PaymentMessage.query.get(message_id)
    return payment.to_dict() if payment else {"error": "Not found"}

def create_payment(data):
    payment = PaymentMessage(**data)
    db.session.add(payment)
    db.session.commit()
    return payment.to_dict()

def update_payment(message_id, data):
    payment = PaymentMessage.query.get(message_id)
    if not payment:
        return {"error": "Not found"}

    for key, value in data.items():
        if hasattr(payment, key):
            setattr(payment, key, value)

    db.session.commit()
    return payment.to_dict()

def delete_payment(message_id):
    payment = PaymentMessage.query.get(message_id)
    if not payment:
        return {"error": "Not found"}

    db.session.delete(payment)
    db.session.commit()
    return {"message": "Deleted"}
