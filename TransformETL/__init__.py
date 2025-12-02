import logging
import json
import os
import time
from datetime import datetime
from io import StringIO

# Bibliotecas do Azure para acesso ao Storage e Identidade Gerida
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Biblioteca para processamento de dados
import pandas as pd


STORAGE_ACCOUNT_URL = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
RAW_CONTAINER = "raw"
PROCESSED_CONTAINER = "processed"

# --------------------------------------------------------------------------------

def process_and_harmonize_data(csv_data: str, output_base: str, run_ts: int):
    """
    Lógica de ETL: Calcula métricas de vendas e estrutura o JSON por dispositivo.
    """
    # Lógica de transformação idêntica à do TransformationNotebook.py
    df = pd.read_csv(StringIO(csv_data))
    
    # Transformação: Calcular Receita e converter tipos
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['revenue'] = df['qty'] * df['unit_price']

    # Derivar Time Window 
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
        records['timestamp'] = records['timestamp'].dt.isoformat()
        
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
        
        #Preparar Nomes dos Ficheiros
        timestamp_folder = datetime.fromtimestamp(run_ts / 1000).strftime('%Y%m%d%H%M%S')
        
        latest_path = f"latest/device-{device_id}.json"
        history_path = f"by-timestamp/{timestamp_folder}/device-{device_id}.json"
        
        # Armazenar o conteúdo JSON para escrita
        output_content = json.dumps(harmonized_data, indent=4)
        
        output_blobs.append({"path": latest_path, "content": output_content})
        output_blobs.append({"path": history_path, "content": output_content})

    return output_blobs

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Função principal ativada por HTTP (ADF Web Activity).
    """
    logging.info('Python HTTP trigger function processed a request for ETL transformation.')

    try:
        # Receber o nome do ficheiro
        req_body = req.get_json()
        input_filename = req_body.get('fileName')
        
        if not input_filename:
            return func.HttpResponse(
                 "Please pass a 'fileName' in the request body",
                 status_code=400
            )
            
        #Inicializar o cliente do Azure Storage
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(STORAGE_ACCOUNT_URL, credential=credential)
        
        # Read csv
        input_blob_path = f"{RAW_CONTAINER}/{input_filename}"
        blob_client = blob_service_client.get_blob_client(RAW_CONTAINER, input_filename)
        
        logging.info(f"Downloading blob: {input_blob_path}")
        download_stream = blob_client.download_blob()
        csv_data = download_stream.readall().decode('utf-8')

    except Exception as e:
        logging.error(f"Error during input/client setup: {e}")
        return func.HttpResponse(
             f"Processing failed during input setup or storage access: {e}",
             status_code=500
        )

    # Transform the data
    run_ts = int(time.time() * 1000)
    try:
        output_blobs = process_and_harmonize_data(csv_data, PROCESSED_CONTAINER, run_ts)
    except Exception as e:
        logging.error(f"Error during data processing: {e}")
        return func.HttpResponse(
             f"Processing failed during Python transformation: {e}",
             status_code=500
        )

    #Write JSON
    try:
        container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER)
        
        for blob_data in output_blobs:
            blob_path = blob_data['path']
            blob_content = blob_data['content']
            
            output_client = container_client.get_blob_client(blob_path)
            output_client.upload_blob(blob_content, overwrite=True)
            logging.info(f"Successfully uploaded: {PROCESSED_CONTAINER}/{blob_path}")
            
    except Exception as e:
        logging.error(f"Error during output writing: {e}")
        return func.HttpResponse(
             f"Processing failed during output writing to Storage: {e}",
             status_code=500
        )

    #Retornar ADF
    return func.HttpResponse(
        json.dumps({"status": "Success", "message": f"Processed {len(output_blobs)} output blobs successfully."}),
        mimetype="application/json",
        status_code=200
    )

if __name__ == '__main__':
    pass