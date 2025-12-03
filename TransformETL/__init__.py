import logging
import json
import os
import time
import azure.functions as func
from datetime import datetime
from io import StringIO

# Bibliotecas do Azure para acesso ao Storage e Identidade Gerida
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd # Biblioteca para processamento de dados

# --- VARIÁVEIS CRÍTICAS ---
# AZURE_STORAGE_ACCOUNT_URL deve ser definido nos Application Settings do Azure Function.
STORAGE_ACCOUNT_URL = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
RAW_CONTAINER = "raw"
PROCESSED_CONTAINER = "processed"

# DADOS DE TESTE ESTRUTURADOS (Substitui a leitura do CSV para debug local)
TEST_CSV_DATA = """device_id,timestamp,item_id,qty,unit_price,store
S-101,2025-09-15T08:00:00+00:00,SKU-S-03,1,19.99,CLUJ-D
S-101,2025-09-15T08:15:00+00:00,SKU-ACC-05,2,9.99,CLUJ-D
S-102,2025-09-15T08:45:00+00:00,SKU-XL-01,2,129.99,CLUJ-D
"""

# --------------------------------------------------------------------------------

def process_and_harmonize_data(csv_data: str, run_ts: int):
    # Lógica de transformação idêntica à do TransformationNotebook.py
    df = pd.read_csv(StringIO(csv_data))
    
    # Transformação: Calcular Receita e converter tipos
    # A leitura direta do CSV é segura aqui porque o seu ficheiro tem o formato correto.
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['revenue'] = df['qty'] * df['unit_price']

    # Derivar Time Window 
    # Usar .isoformat() nos objetos Timestamp agregados (min/max)
    min_time = df['timestamp'].min().isoformat()
    max_time = df['timestamp'].max().isoformat()
    
    time_window = f"{min_time}/{max_time}"
    
    # Agrupar e processar por dispositivo
    grouped = df.groupby('device_id')
    
    output_blobs = []

    for device_id, group_df in grouped:
        
        # Calcular Métricas de Resumo
        total_revenue = group_df['revenue'].sum().round(2)
        total_items_sold = group_df['qty'].sum()
        
        # Preparar Lista de Registos
        records = group_df[['timestamp', 'item_id', 'qty', 'unit_price', 'revenue', 'store']].copy()
        
        # Usar apply(lambda) para garantir que cada item individual é formatado
        records['timestamp'] = records['timestamp'].apply(lambda x: x.isoformat()) 
        
        # Estrutura JSON Harmonizada
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
        if df.empty:
            raise ValueError("CSV file is empty — no records to process.")

        
        # Preparar Nomes dos Ficheiros
        timestamp_folder = datetime.fromtimestamp(run_ts / 1000).strftime('%Y%m%d%H%M%S')
        
        latest_path = f"latest/device-{device_id}.json"
        history_path = f"by-timestamp/{timestamp_folder}/device-{device_id}.json"
        
        # Armazenar o conteúdo JSON para escrita
        output_content = json.dumps(harmonized_data, indent=4)
        
        output_blobs.append({"path": latest_path, "content": output_content})
        output_blobs.append({"path": history_path, "content": output_content})

    return output_blobs

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    logging.info('A1: Function execution started.')

    if not STORAGE_ACCOUNT_URL:
        # Se a variável crucial estiver ausente, retornamos um erro 500 imediato
        logging.error("A2: CRITICAL: AZURE_STORAGE_ACCOUNT_URL is not configured.")
        return func.HttpResponse("Processing failed: Storage Account URL setting is missing.", status_code=500)

    try:
        logging.info(f"A3: URL read from config: {STORAGE_ACCOUNT_URL}")
        
        # Receber o nome do ficheiro (Body)
        req_body = req.get_json()
        input_filename = req_body.get('fileName')
        
        if not input_filename:
            return func.HttpResponse(
                 "Please pass a 'fileName' in the request body",
                 status_code=400
            )
            
        # Inicializar o cliente do Azure Storage com Identidade Gerida
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(STORAGE_ACCOUNT_URL, credential=credential)
        logging.info("A4: Blob Service Client initialized (Managed Identity Check Passed).")
        
        # --- LÓGICA DE TESTE MOCADO ---
        if input_filename == "TEST_RUN":
            csv_data = TEST_CSV_DATA
            logging.info("A5: DEBUG MODE: Using hardcoded CSV data for testing.")
        # --- LÓGICA DE PRODUÇÃO (I/O) ---
        else:
            # Read csv do Storage
            input_blob_path = f"{RAW_CONTAINER}/{input_filename}"
            blob_client = blob_service_client.get_blob_client(RAW_CONTAINER, input_filename)
            
            logging.info(f"A5: Downloading blob: {input_blob_path}")
            # AQUI ESTAVA A FALTAR O CODIGO PARA LER O FICHEIRO
            download_stream = blob_client.download_blob()
            csv_data = download_stream.readall().decode('utf-8')
            logging.info("A6: CSV downloaded successfully.")

    except Exception as e:
        # Captura erros de MI, URL inválido, e BlobNotFound
        logging.error(f"A9: CRITICAL ERROR during input/client setup: {e}")
        return func.HttpResponse(
             f"Processing failed during input setup or storage access: {e}",
             status_code=500
        )

    # Transform the data
    run_ts = int(time.time() * 1000)
    try:
        logging.info("A7: Starting data transformation (Pandas logic).")
        output_blobs = process_and_harmonize_data(csv_data, run_ts) 
    except Exception as e:
        logging.error(f"A10: ERROR during data processing: {e}")
        return func.HttpResponse(
             f"Processing failed during Python transformation: {e}",
             status_code=500
        )

    # Write JSON
    try:
        container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER)
        
        for blob_data in output_blobs:
            blob_path = blob_data['path']
            blob_content = blob_data['content']
            
            output_client = container_client.get_blob_client(blob_path)
            output_client.upload_blob(blob_content, overwrite=True)
            logging.info(f"A8: Successfully uploaded: {PROCESSED_CONTAINER}/{blob_path}")
            
    except Exception as e:
        logging.error(f"A11: ERROR during output writing: {e}")
        return func.HttpResponse(
             f"Processing failed during output writing to Storage: {e}",
             status_code=500
        )

    # Retornar Sucesso ao ADF
    return func.HttpResponse(
        json.dumps({"status": "Success", "message": f"Processed {len(output_blobs)} output blobs successfully."}),
        mimetype="application/json",
        status_code=200
    )