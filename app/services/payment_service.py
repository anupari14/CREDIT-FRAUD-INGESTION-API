# Dummy in-memory store (replace with DB later)
payments = {}

def get_all_payments():
    return list(payments.values())

def get_payment_by_id(message_id):
    return payments.get(message_id, {"error": "Not found"})

def create_payment(data):
    message_id = data.get("message_id")
    if not message_id:
        return {"error": "message_id is required"}
    payments[message_id] = data
    return data

def update_payment(message_id, data):
    if message_id not in payments:
        return {"error": "Not found"}
    payments[message_id].update(data)
    return payments[message_id]

def delete_payment(message_id):
    return payments.pop(message_id, {"error": "Not found"})
