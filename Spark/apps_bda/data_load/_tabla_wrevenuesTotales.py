from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.functions import current_date, when, col, mean 
from pyspark.sql.functions import col, year, month, dayofmonth , lit, concat, substring, to_date

spark = SparkSession.builder \
.appName("SPARK S3") \
.config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
.config("spark.hadoop.fs.s3a.access.key", 'test') \
.config("spark.hadoop.fs.s3a.secret.key", 'test') \
.config("spark.sql.shuffle.partitions", "4") \
.config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
.config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
.master("spark://spark-master:7077") \
.getOrCreate()


try:
    #___________________________________
    # Unir ambos DataFrames en función de la columna común store_ID
    bucket_name = 'my-local-bucket' 
    file_name='stores_csv'
    df_stores = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
    df_stores.show()
    
    # Archivo virtual
    other_file_name='sales_csv'
    df_sales = spark.read.option("header", "true").csv(f"s3a://{bucket_name}/{other_file_name}")
    df_sales.show()
    
    df = df_stores.join(df_sales.select("store_ID","product_ID","revenue"), "store_ID", "left")
    #___________________________________
    #df.show()
    
    
    # Eliminar columnas
    #df = df[[col for col in df.columns if col != "Dia"]]
    
    # Agrupar por store_ID y sumar el revenue
    df_grouped = df.groupBy("location").agg(F.sum("revenue").alias("total_revenue"))
    df_grouped.show()
    
    for row in df_grouped.select("*").collect():
        location = row.location
        total_revenue = row.total_revenue
        print(f"Location: {location}, revenue: {total_revenue}")
        
        
    '''
    df_filtrado = df_filtrado.withColumn(columna_especifica,
    when(col(columna_especifica).cast("float").isNotNull(),
    col(columna_especifica)).otherwise("AAAA"))'''

    # Reemplazar los valores nulos en la columna 'location' con 'Sin definir'
    df_filtrado = df_grouped.withColumn("location",
        when(col("location").isNull(), "Sin definir").otherwise(col("location")))

    
   
    # Escribe el DataFrame como un archivo CSV localmente
    output_file_path = "s3a://my-local-bucket/premio_csv"
    df = df_grouped.write.csv(output_file_path, mode="overwrite", header=True)
    print("LLegado al final")
    
    
    
    spark.stop()
    
except Exception as e:
    print("error reading TXT")
    print(e)
    
    
# Falta crear la tabla y que los inserte con jdbc