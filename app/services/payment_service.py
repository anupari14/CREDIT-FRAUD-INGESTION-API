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

def create_payments_batch(data_list):
    """
    Creates multiple payment records in a single transaction.

    Args:
        data_list (list): A list of dictionaries, where each dictionary represents a payment record.

    Returns:
        dict: A summary of the operation, including success and failure counts.
    """
    payments = []
    failed_records = []
    for data in data_list:
        try:
            payment = PaymentMessage(**data)
            payments.append(payment)
        except Exception as e:
            failed_records.append({"data": data, "error": str(e)})

    if payments:
        try:
            db.session.bulk_save_objects(payments)  # Efficiently save multiple objects
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return {"error": "Failed to save batch", "details": str(e)}

    return {
        "success_count": len(payments),
        "failure_count": len(failed_records),
        "failed_records": failed_records,
    }

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
