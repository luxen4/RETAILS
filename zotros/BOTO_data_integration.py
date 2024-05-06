import boto3  
# Intentar hacerlo con Spark  

def subir_CSV_S3(bucket_name, nom_file):
    
    csv_file_path = "./../../1_data_bda/csv/" + nom_file                    # Path to your CSV file and Read                                             
    with open(csv_file_path, 'rb') as csvfile:
        csv_content = csvfile.read()            

    s3.put_object(Bucket=bucket_name, Key=nom_file, Body=csv_content)       # Upload CSV file to S3 bucket
    print(f"CSV file uploaded to S3 bucket: s3://{bucket_name}/" + nom_file)


# Create a bucket
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')
bucket_name = 'my-local-bucket'                     
s3.create_bucket(Bucket=bucket_name)


nom_file = 'data_clients.csv' 
subir_CSV_S3(bucket_name, nom_file)

nom_file = 'data_employees.csv'
subir_CSV_S3(bucket_name, nom_file)

nom_file = 'data_products.csv' 
subir_CSV_S3(bucket_name, nom_file)

nom_file = 'data_providers.csv' 
subir_CSV_S3(bucket_name, nom_file)

nom_file = 'data_stores.csv'
subir_CSV_S3(bucket_name, nom_file)

nom_file = 'sales_data.csv' 
subir_CSV_S3(bucket_name, nom_file)

# documento donde se realiza el análisis de datos.

# Ver los archivos del bucket   ---> awslocal s3 ls s3://my-local-bucket
# Crear un bucket               ---> awslocal s3api create-bucket --bucket my-local-bucket
