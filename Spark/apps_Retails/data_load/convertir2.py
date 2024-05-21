import boto3

# Crea un cliente de S3
s3_client = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test')

# Ruta local del archivo a subir
local_file_path = '/ruta/a/tu/archivo/sales_data_procesado.csv'

# Ruta en S3 donde se almacenará el archivo
s3_bucket_name = 'my-local-bucket'
s3_key = 'sales_data_procesado.csv'

try:
    # Sube el archivo al cubo S3 local
    s3_client.upload_file(local_file_path, s3_bucket_name, s3_key)
    print(f"Archivo {local_file_path} subido correctamente a {s3_bucket_name}/{s3_key}")

    # Especifica la ruta del directorio virtual a convertir
    source_prefix = 'sales_data_procesado.csv/'

    # Especifica la ruta del archivo normal
    destination_key = 'sales_data_procesado.csv'

    # Copia el contenido del directorio virtual al archivo normal
    response = s3_client.copy_object(
        Bucket=s3_bucket_name,
        CopySource={'Bucket': s3_bucket_name, 'Key': source_prefix},
        Key=destination_key
    )

    # Verifica si la operación fue exitosa
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f"El directorio virtual {source_prefix} ha sido convertido en el archivo {destination_key}")
    else:
        print("Hubo un problema al convertir el directorio virtual en archivo normal")

except Exception as e:
    print("Ocurrió un error:")
    print(e)