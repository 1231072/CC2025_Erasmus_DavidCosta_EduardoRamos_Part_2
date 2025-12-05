import logging
import json
import os
import time
import traceback
import azure.functions as func
from datetime import datetime
from io import StringIO

# Protege imports críticos em bloco try/except para registar falhas de import no log
try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
    import pandas as pd
except Exception as e:
    # Se falhar aqui, a função não inicializa corretamente — vamos registar e re-levantar
    logging.exception("Failed during module import of dependencies.")
    raise

# --- VARIÁVEIS CRÍTICAS ---
STORAGE_ACCOUNT_URL = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
RAW_CONTAINER = "raw"
PROCESSED_CONTAINER = "processed"

TEST_CSV_DATA = """device_id,timestamp,item_id,qty,unit_price,store
S-101,2025-09-15T08:00:00+00:00,SKU-S-03,1,19.99,CLUJ-D
S-101,2025-09-15T08:15:00+00:00,SKU-ACC-05,2,9.99,CLUJ-D
S-102,2025-09-15T08:45:00+00:00,SKU-XL-01,2,129.99,CLUJ-D
"""

def process_and_harmonize_data(csv_data: str, run_ts: int):
    df = pd.read_csv(StringIO(csv_data))
    if df.empty:
        raise ValueError("CSV file is empty — no records to process.")

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['revenue'] = df['qty'] * df['unit_price']

    min_time = df['timestamp'].min().isoformat()
    max_time = df['timestamp'].max().isoformat()
    time_window = f"{min_time}/{max_time}"

    grouped = df.groupby('device_id')
    output_blobs = []

    for device_id, group_df in grouped:
        total_revenue = group_df['revenue'].sum().round(2)
        total_items_sold = group_df['qty'].sum()
        records = group_df[['timestamp', 'item_id', 'qty', 'unit_price', 'revenue', 'store']].copy()
        records['timestamp'] = records['timestamp'].apply(lambda x: x.isoformat())
        harmonized_data = {
            "device_id": str(device_id),
            "generation_timestamp": datetime.fromtimestamp(run_ts / 1000).isoformat(),
            "time_window": time_window,
            "summary": {
                "total_revenue": total_revenue,
                "total_items_sold": int(total_items_sold),
                "record_count": len(records)
            },
            "records": records.to_dict('records')
        }
        timestamp_folder = datetime.fromtimestamp(run_ts / 1000).strftime('%Y%m%d%H%M%S')
        latest_path = f"latest/device-{device_id}.json"
        history_path = f"by-timestamp/{timestamp_folder}/device-{device_id}.json"
        output_content = json.dumps(harmonized_data, indent=4)
        output_blobs.append({"path": latest_path, "content": output_content})
        output_blobs.append({"path": history_path, "content": output_content})

    return output_blobs

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Top-level try/except para garantir logging da exceção com stacktrace
    try:
        logging.info('A1: Function execution started.')

        if not STORAGE_ACCOUNT_URL:
            logging.error("A2: CRITICAL: AZURE_STORAGE_ACCOUNT_URL is not configured.")
            return func.HttpResponse("Processing failed: Storage Account URL setting is missing.", status_code=500)

        logging.info(f"A3: URL read from config: {STORAGE_ACCOUNT_URL}")

        # Ler body de forma defensiva — usando try/except para problemas de parsing
        try:
            req_body = req.get_json()
        except ValueError:
            # Pode ser que o cliente tenha enviado o fileName na query string
            logging.warning("Request body is not valid JSON or empty. Trying query string.")
            req_body = {}

        input_filename = req_body.get('fileName') or req.params.get('fileName')

        if not input_filename:
            logging.error("No fileName provided in body or query string.")
            return func.HttpResponse("Please pass a 'fileName' in the request body or query string", status_code=400)

        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(STORAGE_ACCOUNT_URL, credential=credential)
        logging.info("A4: Blob Service Client initialized (Managed Identity Check Passed).")

        if input_filename == "TEST_RUN":
            csv_data = TEST_CSV_DATA
            logging.info("A5: DEBUG MODE: Using hardcoded CSV data for testing.")
        else:
            input_blob_path = f"{RAW_CONTAINER}/{input_filename}"
            blob_client = blob_service_client.get_blob_client(RAW_CONTAINER, input_filename)
            logging.info(f"A5: Downloading blob: {input_blob_path}")
            try:
                download_stream = blob_client.download_blob()
                csv_data = download_stream.readall().decode('utf-8')
                logging.info("A6: CSV downloaded successfully.")
            except Exception:
                logging.exception("A9: Failed downloading blob. Check MSI and role assignments.")
                return func.HttpResponse(f"Processing failed during input blob download. See logs for details.", status_code=500)

        run_ts = int(time.time() * 1000)
        logging.info("A7: Starting data transformation (Pandas logic).")
        try:
            output_blobs = process_and_harmonize_data(csv_data, run_ts)
        except Exception:
            logging.exception("A10: ERROR during data processing.")
            return func.HttpResponse(f"Processing failed during Python transformation. See logs for details.", status_code=500)

        try:
            container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER)
            for blob_data in output_blobs:
                blob_path = blob_data['path']
                blob_content = blob_data['content']
                try:
                    output_client = container_client.get_blob_client(blob_path)
                    output_client.upload_blob(blob_content, overwrite=True)
                    logging.info(f"A8: Successfully uploaded: {PROCESSED_CONTAINER}/{blob_path}")
                except Exception:
                    logging.exception("A11: Failed uploading output blob")
                    return func.HttpResponse("Processing failed during output writing to Storage. See logs for details.", status_code=500)
        except Exception:
            logging.exception("A11: ERROR during output writing to Storage (container client init)")
            return func.HttpResponse("Processing failed during output writing to Storage. See logs for details.", status_code=500)

        return func.HttpResponse(
            json.dumps({"status": "Success", "message": f"Processed {len(output_blobs)} output blobs successfully."}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as exc:
        # Caso algo se quebre fora dos try/except acima, regista e devolve stacktrace para debug
        tb = traceback.format_exc()
        logging.exception("Unhandled exception in Function main(): %s", exc)
        return func.HttpResponse(
            json.dumps({"status": "Fail", "error": str(exc), "traceback": tb}),
            mimetype="application/json",
            status_code=500
        )
