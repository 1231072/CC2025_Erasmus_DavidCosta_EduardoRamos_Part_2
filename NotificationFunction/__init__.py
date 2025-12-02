import logging
import json
import os
import requests
import azure.functions as func
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "TEAMS_WEBHOOK_URL_HERE")
STORAGE_ACCOUNT_URL = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
PROCESSED_CONTAINER = "processed"

def validate_harmonized_schema(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    
    required_keys = ["device_id", "generation_timestamp", "time_window", "summary", "records"]
    if not all(key in data for key in required_keys):
        logging.error("Validation failed: Missing core keys.")
        return False

    summary = data.get("summary", {})
    
    if "total_revenue" not in summary or "record_count" not in summary:
        logging.error("Validation failed: Missing 'total_revenue' or 'record_count' in summary.")
        return False
        
    if summary.get("record_count", 0) <= 0 or summary.get("total_revenue", 0) <= 0:
        logging.warning(f"Validation failed: Zero revenue or records found for device {data.get('device_id')}.")
        return False
        
    return True

def send_teams_notification(payload):
    if TEAMS_WEBHOOK_URL == "TEAMS_WEBHOOK_URL_HERE":
        logging.warning("TEAMS_WEBHOOK_URL is not configured. Skipping notification.")
        return

    try:
        response = requests.post(TEAMS_WEBHOOK_URL, headers={'Content-Type': 'application/json'}, data=json.dumps(payload))
        response.raise_for_status()
        logging.info("Teams notification sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Teams notification: {e}")

def main(event: func.EventGridEvent):
    logging.info('Python Event Grid trigger function (Notification) started.')

    result = json.loads(event.get_json())
    url = result.get('url')
    
    is_schema_valid = False
    file_data = None
    device_id = 'N/A'
    revenue = 0
    error_detail = None
    
    if not url or "latest/" not in url or not url.endswith(".json"):
        logging.warning(f"Ignoring event for non-final file: {url}")
        return

    try:
        if not STORAGE_ACCOUNT_URL:
            raise ValueError("AZURE_STORAGE_ACCOUNT_URL is not configured.")
            
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(STORAGE_ACCOUNT_URL, credential=credential)
        
        blob_name = url.split(f'/{PROCESSED_CONTAINER}/')[-1]
        blob_client = blob_service_client.get_blob_client(PROCESSED_CONTAINER, blob_name)
        
        logging.info(f"Attempting to download and validate: {blob_name}")
        download_stream = blob_client.download_blob()
        file_content = download_stream.readall().decode('utf-8')
        file_data = json.loads(file_content)

        device_id = file_data.get('device_id', 'Unknown')
        revenue = file_data.get('summary', {}).get('total_revenue', 0)
        
        is_schema_valid = validate_harmonized_schema(file_data)
        
    except ValueError as ve:
        error_detail = f"Configuration Error: {ve}"
        logging.error(error_detail)
        is_schema_valid = False
        
    except requests.exceptions.HTTPError as he:
        error_detail = f"Storage Access Error (403 Forbidden?): {he}"
        logging.error(error_detail)
        is_schema_valid = False

    except Exception as e:
        error_detail = f"Critical Error (JSON/IO): {e}"
        logging.error(error_detail)
        is_schema_valid = False
        
    status_text = "SUCCESSFUL PROCESSING" if is_schema_valid else "VALIDATION FAILED"
    color = "00FF00" if is_schema_valid else "FF0000"
    
    details = f"Caminho do Ficheiro: {url}"
    if error_detail:
        details = f"MOTIVO DA FALHA: {error_detail}. {details}"


    teams_payload = {
        "title": f"ETL Pipeline: Status {status_text}",
        "text": f"Reporte de processamento para o ficheiro {os.path.basename(url)}.",
        "sections": [
            {
                "activityTitle": f"Dispositivo {device_id}",
                "activitySubtitle": f"Geração: {datetime.now().isoformat()}",
                "facts": [
                    { "name": "Status Final:", "value": status_text },
                    { "name": "Receita Total (Resumo):", "value": f"{revenue} (Apenas se válido)" },
                    { "name": "Detalhes:", "value": details }
                ],
                "markdown": True
            }
        ],
        "themeColor": color
    }
    
    send_teams_notification(teams_payload)