import os
from google.cloud import bigquery

def export_dataset_to_gcs(event, context):
    """
    Cloud Function para exportar todas las tablas de un conjunto de datos de BigQuery
    a un bucket de Cloud Storage en formato Parquet.

    La configuración se obtiene de variables de entorno.
    Se espera ser activada por un evento (e.g., Pub/Sub).
    """
    project_id = os.environ.get('GCP_PROJECT')
    dataset_id = os.environ.get('SOURCE_DATASET_ID')
    bucket_name = os.environ.get('DESTINATION_BUCKET_NAME')
    location = os.environ.get('BIGQUERY_LOCATION', 'US') # Ubicación por defecto si no se especifica

    if not project_id or not dataset_id or not bucket_name:
        print("Error: GCP_PROJECT, SOURCE_DATASET_ID y DESTINATION_BUCKET_NAME deben estar definidos en las variables de entorno.")
        return

    print(f"Iniciando exportación para el dataset: {dataset_id} en el proyecto {project_id} hacia el bucket: {bucket_name}")

    client = bigquery.Client(project=project_id, location=location)

    try:
        # Obtener referencia al dataset
        dataset_ref = client.dataset(dataset_id, project=project_id)

        # Listar todas las tablas en el dataset
        tables = list(client.list_tables(dataset_ref))

        if not tables:
            print(f"No se encontraron tablas en el dataset: {dataset_id}")
            return

        print(f"Encontradas {len(tables)} tablas en el dataset.")

        for table in tables:
            table_id = table.table_id
            table_ref = dataset_ref.table(table_id)

            # Definir la URI de destino en GCS
            # gs://[BUCKET_NAME]/[DATASET_ID]/[TABLE_ID]/*.parquet
            # El '*' es importante para que BigQuery divida la salida si es muy grande
            destination_uri = f"gs://{bucket_name}/{dataset_id}/{table_id}/*.parquet"

            print(f"Exportando tabla {table_id} a {destination_uri}")

            # Configurar el trabajo de exportación
            job_config = bigquery.ExtractJobConfig()
            job_config.destination_format = bigquery.DestinationFormat.PARQUET
            # job_config.compression = bigquery.Compression.SNAPPY # SNAPPY es el default para Parquet

            # Iniciar el trabajo de exportación
            # Los trabajos son asíncronos, la función terminará después de iniciar el trabajo
            extract_job = client.extract_table(
                table_ref,
                destination_uri,
                job_config=job_config,
                # Se puede añadir un job_id opcional, de lo contrario se generará uno
                # job_id="mi_export_job_" + table_id + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
            )

            print(f"Trabajo de exportación iniciado para la tabla {table_id}. Job ID: {extract_job.job_id}")
            # Nota: No esperamos a que el trabajo termine dentro de la Cloud Function
            # para evitar timeouts en funciones de corta duración. El trabajo se ejecuta en BigQuery.


    except Exception as e:
        print(f"Error durante la exportación: {e}")
        # En un entorno de producción, podrías querer enviar una notificación de error aquí.

    print("Proceso de inicio de exportación completado.")

# La función 'event' y 'context' son parámetros estándar para Cloud Functions
# activadas por eventos (como Pub/Sub). Aunque no los usemos directamente aquí,
# la firma de la función debe incluirlos si se activa por Pub/Sub.
