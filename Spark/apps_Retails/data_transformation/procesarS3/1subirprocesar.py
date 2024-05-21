import boto3

# Configure boto3 to use LocalStack endpoint
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',               # dummy access key
    aws_secret_access_key='test',           # dummy secret key
    region_name='us-east-1'                 # region name, required but not used by LocalStack
)

# Create a bucket if it doesn't exist
bucket_name = 'my-local-bucket'
if 'Buckets' in s3.list_buckets() and bucket_name not in [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket_name)

# Path to your Python script file
script_file_path = '2procesar.py'

# Read script file content
with open(script_file_path, 'rb') as script_file:
    script_content = script_file.read()

# Upload script file to S3 bucket
s3.put_object(Bucket=bucket_name, Key='2procesar.py', Body=script_content)

print(f"Python script uploaded to S3 bucket: s3://{bucket_name}/2procesar.py")