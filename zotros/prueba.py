from pyspark.sql import SparkSession

# Inicializa SparkSession
spark = SparkSession.builder \
    .appName("Procesamiento de ventas") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

try:
    # Lee el archivo CSV
    df = spark.read.csv("s3a://my-local-bucket/sales_data.csv", header=True)

    # Procesa el DataFrame según sea necesario
    df_procesado = df.filter(df['columna'] > 100)

    # Escribe el DataFrame procesado como un nuevo archivo CSV
    df_procesado.write.csv("s3a://my-local-bucket/sales_data_procesado.csv", header=True)

    print("Procesamiento completado. Nuevo archivo CSV creado.")

except Exception as e:
    print("Ocurrió un error durante el procesamiento:", e)

finally:
    spark.stop()