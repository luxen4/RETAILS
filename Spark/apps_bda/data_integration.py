from pyspark.sql import SparkSession

# Crear la sesión de Spark
def sesionSpark():
    spark = SparkSession.builder \
    .appName("Leer y procesar con Spark") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
    
    return spark

# Meter directamente los archivos a S3
try:
    spark=sesionSpark()
    
    # csv
    df = spark.read.csv("./../spark-data/csv/sales_data.csv")
    ruta_salida = "s3a://my-local-bucket/sales_data.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")
    
    
    '''
    # csv
    df = spark.read.csv("./../spark-bda/csv/generar/habitaciones2.csv")
    ruta_salida = "s3a://my-local-bucket/habitaciones_data.csv"
    df=df.write.csv(ruta_salida, mode="overwrite")
    
    # json
    df = spark.read.option("multiline", "true").json("./../spark-data/json/restaurantes.json")
    ruta_salida = "s3a://my-local-bucket/restaurantes_data.json"
    df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")'''
    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)




# Crear el bucket por comando
#   awslocal s3api create-bucket --bucket my-local-bucket

# Listar los archivos del bucket
#   awslocal s3 ls s3://my-local-bucket

### OK ###


'''
import boto3    
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',region_name='us-east-1')

bucket_name = 'my-local-bucket'                     # Create a bucket
s3.create_bucket(Bucket=bucket_name)

csv_file_path = 'sales_data.csv'                    # Path to your CSV file and Read                                             
with open(csv_file_path, 'rb') as csvfile:
    csv_content = csvfile.read()            

s3.put_object(Bucket=bucket_name, Key='sales_data.csv', Body=csv_content)       # Upload CSV file to S3 bucket
print(f"CSV file uploaded to S3 bucket: s3://{bucket_name}/sales_data.csv")
'''
