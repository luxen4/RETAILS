import psycopg2
import boto3
import csv
import pandas as pd

def selectStores():
    try:
        #connection = psycopg2.connect(host='my_postgres_service', port='5432', database='retail_db', user= 'postgres', password='casa1234' )
        connection = psycopg2.connect(host='localhost', port='5432', database='retail_db', user= 'postgres', password='casa1234' )
        
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Stores;")
        rows = cursor.fetchall()

        print("Datos en la tabla 'Stores':")
        for row in rows:
            print(row)

        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)

selectStores()


def exportar_a_csv_y_subir_a_s3():
   
    try:
        #connection = psycopg2.connect(host='my_postgres_service', port='5432', database='retail_db', user= 'postgres', password='casa1234' )
        connection = psycopg2.connect(host='localhost', port='5432', database='retail_db', user= 'postgres', password='casa1234' )
        cursor = connection.cursor()
        
        cursor.execute("SELECT * FROM stores")
        rows = cursor.fetchall()
        
        csv_file_path = "datos_exportados.csv"
        
                                                                            # Escribir los datos en el archivo CSV
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            column_names = [desc[0] for desc in cursor.description]         # Escribir encabezados de columnas
            csv_writer.writerow(column_names)
            csv_writer.writerows(rows)                                      # Escribir filas de datos
        
        print("Datos exportados correctamente a:", csv_file_path)
        
                                                                            # Subir el archivo CSV al bucket de S3
        s3_client = boto3.client('s3', endpoint_url='http://localhost:4566', 
                                 aws_access_key_id='test', 
                                 aws_secret_access_key='test',)

        bucket_name = 'my-local-bucket'                                     # Create a bucket
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.upload_file(csv_file_path, bucket_name, 'stores_exportados.csv')
        
        print("Archivo CSV subido correctamente a S3.")
        
    except (psycopg2.Error, Exception) as e:
        print("Error:", e)
    finally:
        # Cerrar cursor y conexión
        print()
        cursor.close()
        connection.close()
        
        
def leer_csv_desde_s3(bucket_name, file_name):
    try:
        # Inicializar cliente de S3
        s3_client = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',)

        local_file_path = 'temp.csv'
        s3_client.download_file(bucket_name, file_name, local_file_path)
        df = pd.read_csv(local_file_path, encoding='latin1')
        print(df.head())

        return df

    except Exception as e:
        print("Error:", e)
        return None
    
# Llamar a la función para exportar datos a CSV y subirlos a S3
exportar_a_csv_y_subir_a_s3()


# Llamar a la función para leer el archivo CSV desde S3
bucket_name = 'my-local-bucket'
file_name = 'stores_exportados.csv'
df = leer_csv_desde_s3(bucket_name, file_name)