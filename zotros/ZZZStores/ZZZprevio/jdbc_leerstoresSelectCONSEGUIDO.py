from pyspark.sql import SparkSession

# Leer un Dataframe
def leerResultados(resultado):
    store_id = None
    if not resultado.isEmpty():
        row = resultado.collect()[0]
        store_id = row["store_id"]
        store_name = row["store_name"]
        location = row["location"]
        demographics = row["demographics"]
        
        print("store_id:", store_id)
        print("store_name:", store_name)
        print("location:", location)
        print("demographics:", demographics)
        print()

def select():
    # Iniciar sesión de Spark
    spark = SparkSession.builder \
        .appName("SPARK S3") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
        .config("spark.hadoop.fs.s3a.access.key", 'test') \
        .config("spark.hadoop.fs.s3a.secret.key", 'test') \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.committer.name", "directory") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0") \
        .config("spark.jars","postgresql-42.7.3.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
        
        
    # Definir propiedades de conexión
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="stores", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    #resultado = spark.sql("SELECT * FROM tabla_spark WHERE store_name ='" + store_name + "';")
    resultado = spark.sql("SELECT * FROM tabla_spark;")
    resultado.show()
    
    
    #resultado.write.csv("s3a://my-local-bucket/stores.csv", header=True, mode="overwrite")
    
    # Escribir el DataFrame en un archivo CSV en el bucket S3 local
    # df.write.option("header", "true").csv("s3a://my-local-bucket/stores.csv")

    
    # Escribir el DataFrame en un archivo CSV local
    resultado.write.option("header", "true").csv("resultado.csv")

    # Copiar el archivo CSV al bucket S3 local
    import os
    os.system("awslocal s3 cp resultado.csv s3://my-local-bucket/stores.csv")
        
    
    resultado \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://my-local-bucket/stores.csv', sep=',')

    
    
    
    leerResultados(resultado)
    spark.stop()

# select('OKG')
select()


### Postgress ###

# psql -U postgres
# create database retail_db;      # create database warehouse_retail_db;
# \c retail_db                    # \c warehouse_retail_db
# \l                                -> para ver las bases de datos
# \dt                               -> para ver las tablas
# DROP DATABASE mi_base_de_datos;
# \q o \quit
# DROP TABLE IF EXISTS stores;

