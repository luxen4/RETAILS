import boto3
s3 = boto3.client('s3',endpoint_url='http://localhost:4566',aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

bucket_name = 'my-local-bucket'                             # Bucket and file details
file_key = 'sales_data.csv'
response = s3.get_object(Bucket=bucket_name, Key=file_key)  # Get the object from S3

content = response['Body'].read().decode('utf-8')           # Read and decode the content
print(content)                                              # Print the content

print(f"File '{file_key}' downloaded from s3://{bucket_name}/ whose values is {content}")
