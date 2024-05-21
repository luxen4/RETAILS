from pyspark.sql import SparkSession

# Crear la sesión de Spark
def sesionSpark():
    spark = SparkSession.builder \
    .appName("Leer y procesar con Spark") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.jars","/opt/spark-apps/a_jars/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/a_jars/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()
    
    return spark

# Meter directamente los archivos a S3
try:
    spark=sesionSpark()
    
    # csv
    #df = spark.read.csv("./../../spark-data/csv/sales_data.csv")
    df = spark.read.csv("/opt/spark-data/csv/sales_data.csv") 
    ruta_salida = "s3a://my-local-bucket/sales_csv"
    df=df.write.csv(ruta_salida, mode="overwrite") #  Ok sin header=True,
    
    bucket_name = 'my-local-bucket' 
    file_name='sales.csv'
    df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
    df_original.show()
    
    # json
    #df = spark.read.option("multiline", "true").json("./../../spark-data/json/data_products.json")
    df = spark.read.option("multiline", "true").json("/opt/spark-data/json/data_products.json")
    ruta_salida = "s3a://my-local-bucket/products_json"
    df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
    
    
    # Leer el archivo
    bucket_name = 'my-local-bucket'
    file_name='products_json'
    df_original = spark.read.json(f"s3a://{bucket_name}/{file_name}")
    df_original.show()
    
    
    
    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

### OK ###


