from pyspark.sql import SparkSession

# Bucket and file details
bucket_name = 'my-local-bucket'
input_file_key = 'example.csv'
output_file_key = 'processed_example.csv'

# Path to Spark local directory
spark_local_dir = '/tmp/spark_local'

# Create Spark session
spark = SparkSession.builder \
    .appName("CSV Processing") \
    .config("spark.local.dir", spark_local_dir) \
    .getOrCreate()

# Read CSV file directly from LocalStack S3 endpoint
s3_path = f"s3a://localhost:4566/{bucket_name}/{input_file_key}"
df = spark.read.csv(s3_path, header=True, inferSchema=True)