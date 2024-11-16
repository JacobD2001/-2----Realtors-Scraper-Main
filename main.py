from flask import Flask, jsonify
import os
import requests
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()
APIFY_TOKEN = os.getenv('APIFY_TOKEN')
NOCODB_TOKEN = os.getenv('NOCODB_TOKEN')
NOCODB_URL = os.getenv('NOCODB_URL')
TABLE_ID = os.getenv('NOCODB_TABLE_ID')

APIFY_DATASET_URL = "https://api.apify.com/v2/acts/jupri~realtor-agents/runs/last/dataset/items"

app = Flask(__name__)

def fetch_apify_data() -> List[Dict]:
    """Fetch data from Apify API"""
    response = requests.get(APIFY_DATASET_URL, params={'token': APIFY_TOKEN})
    if response.status_code != 200:
        raise Exception("Failed to fetch data from Apify")
    return response.json()

def save_to_nocodb(data: List[Dict]) -> bool:
    """Save data to NocoDB"""
    headers = {
        'xc-token': NOCODB_TOKEN,
        'Content-Type': 'application/json'
    }
    
    endpoint = f"https://{NOCODB_URL}/api/v2/tables/{TABLE_ID}/records"
    response = requests.post(endpoint, headers=headers, json=data)
    
    return response.status_code == 200

@app.route("/run-app", methods=['POST'])
def run_app():
    try:
        # Fetch data from Apify
        apify_data = fetch_apify_data()
        
        # Save to NocoDB
        success = save_to_nocodb(apify_data)
        
        if success:
            return jsonify({"status": "Data saved successfully"})
        else:
            return jsonify({"status": "Failed to save data"}), 500
            
    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8000)))

