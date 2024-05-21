from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, when, col, mean 
from pyspark.sql.functions import col, year, month, dayofmonth , lit, concat, substring, to_date, min

# Configurar SparkSession
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Nombre del archivo en S3
bucket_name = "my-local-bucket"
file_key = "sales_data_procesado.csv"

# Ruta completa del archivo en S3
s3_path = f"s3a://my-local-bucket/{file_key}"
df = spark.read.csv(s3_path, header=True, inferSchema=True)




# Convertir la columna 'revenue' a numérica
df = df.withColumn("revenue", col("revenue").cast("double"))

# Extraer el año de la columna 'Año' (ya es numérica)
df = df.withColumn("year", col("Año"))

# Agrupar por año y sumar el revenue
revenue_por_año = df.groupBy("year").agg({"revenue": "sum"}).orderBy("year")


# Mostrar el DataFrame resultante
revenue_por_año.show()





from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, min

# Configurar SparkSession
spark = SparkSession.builder \
    .appName("Calculate Minimum Revenue by Location with Spark") \
    .getOrCreate()

# Leer el archivo CSV desde S3 en un DataFrame de Spark
df = spark.read \
    .option("header", "true") \
    .csv("s3a://my-local-bucket/premio.csv")

# Convertir la columna 'total_revenue' a tipo numérico
df = df.withColumn("total_revenue", df["total_revenue"].cast("double"))

# Agrupar por localidad y sumar el revenue para cada una
revenue_por_localidad = df.groupBy("location").agg(sum("total_revenue").alias("total_revenue"))


# Calcular el mínimo valor de revenue por localidad
revenue_minimo_por_localidad = revenue_por_localidad.groupBy("location").agg(min("total_revenue").alias("min_revenue"))
print("El mínimo revenue para Vitoria es:", revenue_minimo_por_localidad.min_revenue)
# Filtrar las filas donde la localidad sea "Vitoria"
revenue_vitoria = revenue_minimo_por_localidad.filter(revenue_minimo_por_localidad["location"] == "Vitoria").collect()

# Imprimir el mínimo revenue para Vitoria
if revenue_vitoria:
    min_revenue_vitoria = revenue_vitoria[0]["min_revenue"]
    print("El mínimo revenue para Vitoria es:", min_revenue_vitoria)
else:
    print("No se encontraron datos para la localidad de Vitoria")

# Finalizar la sesión de Spark




# Configurar SparkSession
spark = SparkSession.builder \
    .appName("Calculate Minimum Revenue by Location with Spark") \
    .getOrCreate()

# Leer el archivo CSV desde S3 en un DataFrame de Spark
df = spark.read \
    .option("header", "true") \
    .csv("s3a://my-local-bucket/premio.csv")

# Convertir la columna 'total_revenue' a tipo numérico
df = df.withColumn("total_revenue", df["total_revenue"].cast("double"))

# Calcular el mínimo valor de revenue
min_revenue = df.agg(min("total_revenue").alias("min_revenue")).collect()[0]["min_revenue"]

# Imprimir el mínimo revenue
print("El mínimo revenue es:", min_revenue)

# Finalizar la sesión de Spark
spark.stop()













spark.stop()











# Finalizar la sesión de Spark
spark.stop()
