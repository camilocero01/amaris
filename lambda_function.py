
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    
    # Configuración
    source_bucket = 'source-energy-company-camilo'
    dest_bucket = 'datalake-energy-company-camilo'
    
    # Fecha actual
    now = datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    
    # Archivos a procesar
    files = [
        'proveedores.csv',
        'clientes.csv', 
        'transacciones.csv'
    ]
    
    results = []
    
    for file_name in files:
        try:
            # Definir paths
            source_key = file_name
            dest_key = f"raw/clientes/year={year}/month={month}/day={day}/{file_name}"
            
            # Copiar archivo
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            
            results.append(f"Copiado: {file_name}")
            
        except Exception as e:
            results.append(f"Error con {file_name}: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Proceso completado',
            'results': results
        })
    }