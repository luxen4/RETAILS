import boto3
import subprocess

# Configure boto3 to use LocalStack endpoint
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',               # dummy access key
    aws_secret_access_key='test',           # dummy secret key
    region_name='us-east-1'                 # region name, required but not used by LocalStack
)

# S3 bucket and file information
bucket_name = 'my-local-bucket'
script_key = '2procesar.py'

# Download the script from S3
response = s3.get_object(Bucket=bucket_name, Key=script_key)
script_content = response['Body'].read()

# Write the script content to a local Python file
local_script_path = 'local_sales_dataprocesado.py'
with open(local_script_path, 'wb') as f:
    f.write(script_content)

# Submit the script using spark-submit
spark_submit_command = [
    'spark-submit',
    '--master', 'spark://spark-master:7077',
    '--packages', 'org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11',
    '--conf', 'spark.hadoop.fs.s3a.endpoint=http://spark-localstack-1:4566',
    '--conf', 'spark.hadoop.fs.s3a.access.key=test',
    '--conf', 'spark.hadoop.fs.s3a.secret.key=test',
    '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
    '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
    '--conf', 'spark.driver.extraClassPath=/opt/spark/jars/s3-2.25.11.jar',
    '--conf', 'spark.executor.extraClassPath=/opt/spark/jars/s3-2.25.11.jar',
    local_script_path
]

try:
    subprocess.run(spark_submit_command, check=True)
    print("Script execution completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Script execution failed: {e}")