import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')



bucket_name = 'my-local-bucket'
file_key = 'sales_data.csv'

response = s3.get_object(Bucket=bucket_name, Key=file_key)
data = response['Body'].read().decode('utf-8')

# Leer el archivo CSV
df = pd.read_csv(StringIO(data))

# Convertir la columna 'revenue' a numérica si no lo es ya
df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

# Agrupar por store_ID y sumar el revenue
revenue_por_store = df.groupby('store_ID')['revenue'].sum()

# Convertir la serie resultante a un DataFrame y resetear el índice
revenue_por_store = revenue_por_store.reset_index()
print(revenue_por_store)




# Leer el archivo CSV
df = pd.read_csv(StringIO(data))

# Convertir la columna 'revenue' a numérica si no lo es ya
df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

# Extraer el año de la columna 'fecha_procesado'
df['year'] = pd.to_datetime(df['date']).dt.year

# Agrupar por año y sumar el revenue
revenue_por_año = df.groupby('year')['revenue'].sum()

print(revenue_por_año)









