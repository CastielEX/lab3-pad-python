from pymongo import MongoClient
from flask import Flask, request, jsonify
from dotenv import load_dotenv 
import os
import random
import argparse

app = Flask(__name__)


connection_string: str = os.environ.get("CONNECTION_STRING")
mongo_client: MongoClient = MongoClient(connection_string)
load_dotenv()
database = mongo_client.get_database("warehouse_db")
collection = database.get_collection("data")

data = {"key": "Default", "value": "default"}
collection.insert_one(data)


client = MongoClient(connection_string)
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Funcția de load balancing # exemplu
def load_balance():
    servers = ["server1", "server2", "server3"]
    selected_server = random.choice(servers)
    return selected_server

# Funcția de smart-proxy
def smart_proxy(data_key):
    # Verificăm dacă datele sunt în cache (în cazul nostru, stocarea în memorie temporară)
    if data_key in cache:
        return cache[data_key], 'cache'

    # Dacă datele nu sunt în cache, le extragem din MongoDB
    server = load_balance()
    data_from_db = collection.find_one({'key': data_key})

    # Verificăm dacă am găsit date în MongoDB
    if data_from_db is not None and 'value' in data_from_db:
        # Salvăm datele în cache pentru viitoarele cereri
        cache[data_key] = data_from_db['value']
        return data_from_db['value'], 'database'
    else:
        return 'Data not found', 'database'

# Ruta pentru nodul proxy
@app.route('/proxy', methods=['GET', 'POST'])
def proxy():
    if request.method == 'GET':
        data_key = request.args.get('key', 'example_key')
        data, source = smart_proxy(data_key)
        return jsonify({'data': data, 'source': source})
    elif request.method == 'POST':
        posted_data = request.json

        # Verificăm dacă cheia și valoarea sunt prezente în datele trimise
        if 'key' in posted_data and 'value' in posted_data:
            # Actualizăm sau adăugăm datele în MongoDB
            data_key = posted_data['key']
            data_value = posted_data['value']
            collection.update_one({'key': data_key}, {'$set': {'value': data_value}}, upsert=True)

            return jsonify({'message': f'Data for key {data_key} updated/created successfully'})
        else:
            return jsonify({'error': 'Invalid data format for POST request'})

# Ruta pentru afișarea tuturor cheilor și valorilor
@app.route('/all_data', methods=['GET'])
def get_all_data():
    all_data = list(collection.find({}, {'_id': 0}))
    return jsonify({'all_data': all_data})

if __name__ == '__main__':
    # Inițializarea cache-ului (în cazul nostru, un dicționar în care stocăm temporar datele)
    cache = {}

    # Procesarea argumentelor de linie de comandă pentru a primi portul din exterior
    parser = argparse.ArgumentParser(description="Run the Flask application.")
    parser.add_argument("--port", type=int, default=5000, help="Port number")
    args = parser.parse_args()

    # Pornirea serverului Flask pe portul specificat
    app.run(port=args.port, debug=True)
