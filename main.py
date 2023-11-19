from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient("mongodb://localhost:27017/")
db = client["customer_db"]
customers_collection = db["customers"]
purchase_order_collection = db["purchase_order"]
shipping_details_collection = db["shipping_details"]

def validate_customer_data(data):
    required_fields = ["Customer Name", "Email", "Mobile Number", "City"]
    for field in required_fields:
        if field not in data:
            return False
    return True

def validate_purchase_order_data(data):
    required_fields = ["Product Name", "Quantity", "Pricing", "MRP", "Customer ID"]
    for field in required_fields:
        if field not in data:
            return False
    if data["Pricing"] > data["MRP"]:
        return False
    return True

def validate_shipping_details_data(data):
    required_fields = ["Address", "City", "Pincode", "Purchase Order ID", "Customer ID"]
    for field in required_fields:
        if field not in data:
            return False
    return True

@app.route('/add_customer', methods=['POST'])
def add_customer():
    data = request.json
    if not validate_customer_data(data):
        return jsonify({"error": "Invalid customer data"}), 400

    # Perform insertion to the customers collection after validation
    # customers_collection.insert_one(data)

    return jsonify({"message": "Customer added successfully"}), 200


# API to add purchase order
@app.route('/add_purchase_order', methods=['POST'])
def add_purchase_order():
    data = request.json
    if not validate_purchase_order_data(data):
        return jsonify({"error": "Invalid purchase order data or pricing is greater than MRP"}), 400
    return jsonify({"message": "Purchase order added successfully"}), 200


# API to add shipping details
@app.route('/add_shipping_details', methods=['POST'])
def add_shipping_details():
    data = request.json
    if not validate_shipping_details_data(data):
        return jsonify({"error": "Invalid shipping details data"}), 400
    return jsonify({"message": "Shipping details added successfully"}), 200


# API to get customers based on city
@app.route('/get_customers_by_city/<city>', methods=['GET'])
def get_customers_by_city(city):
    pipeline = [
        {"$match": {"City": city}},
        {"$lookup": {
            "from": "purchase_order",
            "localField": "Customer ID",
            "foreignField": "Customer ID",
            "as": "purchase_orders"
        }},
        {"$lookup": {
            "from": "shipping_details",
            "localField": "Customer ID",
            "foreignField": "Customer ID",
            "as": "shipment_details"
        }}
    ]
    customers = list(customers_collection.aggregate(pipeline))
    return jsonify({"customers": customers}), 200


# API to get customers with all purchase orders
@app.route('/get_customers_with_purchase_orders', methods=['GET'])
def get_customers_with_purchase_orders():
    pipeline = [
        {"$lookup": {
            "from": "purchase_order",
            "localField": "Customer ID",
            "foreignField": "Customer ID",
            "as": "purchase_orders"
        }}
    ]
    customers = list(customers_collection.aggregate(pipeline))
    return jsonify({"customers": customers}), 200


# API to get customers with all purchase orders and shipment details
@app.route('/get_customers_with_purchase_orders_and_shipment', methods=['GET'])
def get_customers_with_purchase_orders_and_shipment():
    pipeline = [
        {"$lookup": {
            "from": "purchase_order",
            "localField": "Customer ID",
            "foreignField": "Customer ID",
            "as": "purchase_orders"
        }},
        {"$lookup": {
            "from": "shipping_details",
            "localField": "purchase_orders.Purchase Order ID",
            "foreignField": "Purchase Order ID",
            "as": "shipment_details"
        }}
    ]
    customers = list(customers_collection.aggregate(pipeline))
    return jsonify({"customers": customers}), 200


if __name__ == '__main__':
    app.run(debug=True)
