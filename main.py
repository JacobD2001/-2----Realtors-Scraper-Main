from flask import Flask, jsonify
import os
import requests
import logging
from typing import List, Dict
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
APIFY_TOKEN = os.getenv('APIFY_TOKEN')
NOCODB_TOKEN = os.getenv('NOCODB_TOKEN')
NOCODB_URL = os.getenv('NOCODB_URL')
TABLE_ID = os.getenv('NOCODB_TABLE_ID')

APIFY_DATASET_URL = "https://api.apify.com/v2/acts/jupri~realtor-agents/runs/last/dataset/items"

app = Flask(__name__)

def fetch_apify_data() -> List[Dict]:
    """Fetch data from Apify API"""
    logger.info("Fetching data from Apify API")
    try:
        response = requests.get(APIFY_DATASET_URL, params={'token': APIFY_TOKEN})
        if response.status_code != 200:
            logger.error(f"Apify API request failed with status code: {response.status_code}")
            raise Exception("Failed to fetch data from Apify")
        
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} records from Apify")
        logger.debug(f"First record sample: {str(data[0]) if data else 'No data'}")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Apify: {str(e)}")
        raise

def save_to_nocodb(data: List[Dict]) -> bool:
    """Save data to NocoDB"""
    logger.info(f"Attempting to save {len(data)} records to NocoDB")
    headers = {
        'xc-token': NOCODB_TOKEN,
        'Content-Type': 'application/json'
    }
    
    endpoint = f"https://{NOCODB_URL}/api/v2/tables/{TABLE_ID}/records"
    try:
        response = requests.post(endpoint, headers=headers, json=data)
        if response.status_code == 200:
            logger.info("Successfully saved data to NocoDB")
            return True
        else:
            logger.error(f"Failed to save to NocoDB. Status code: {response.status_code}")
            logger.debug(f"NocoDB error response: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error saving to NocoDB: {str(e)}")
        return False

@app.route("/run-app", methods=['POST'])
def run_app():
    logger.info("Starting data processing pipeline")
    try:
        # Fetch data from Apify
        apify_data = fetch_apify_data()
        
        # Save to NocoDB
        success = save_to_nocodb(apify_data)
        
        if success:
            logger.info("Pipeline completed successfully")
            return jsonify({"status": "Data saved successfully"})
        else:
            logger.error("Pipeline failed during NocoDB save")
            return jsonify({"status": "Failed to save data"}), 500
            
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        return jsonify({"status": "Error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    logger.info(f"Starting Flask application on port {port}")
    app.run(host='0.0.0.0', port=port)

