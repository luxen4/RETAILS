from pyspark.sql import SparkSession

# Crear una sesión de Spark
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
        .config("spark.driver.extraClassPath", "/opt/spark-apps/a_jars/hadoop-aws-3.4.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark-apps/a_jars/hadoop-aws-3.4.0.jar") \
        .config("spark.jars","/opt/spark-apps/a_jars/postgresql-42.7.3.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/a_jars/postgresql-42.7.3.jar") \
        .master("local[*]") \
        .getOrCreate()

    return spark

        
def leerPostgres():
    spark = sesionSpark()
        
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/bdaretails"  # OK pero hay que ponerlo bien borrando todo el docker, no es bastante con el build
    connection_properties = {"user": "user1", "password": "retails", "driver": "org.postgresql.Driver"}


    df = spark.read.jdbc(url=jdbc_url, table="stores", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    resultado = spark.sql("SELECT * FROM tabla_spark;")
    resultado.show()
    
    
    resultado \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .json(path='s3a://my-local-bucket/stores_json')                   ### OK ###
    
    # Leer el archivo
    bucket_name = 'my-local-bucket'
    file_name='stores_json'
    df_original = spark.read.json(f"s3a://{bucket_name}/{file_name}")
    df_original.show()
    
    
    
    spark.stop()

leerPostgres()

# resultado.write.csv("s3a://my-local-bucket/empleados1.csv", header=True, mode="overwrite")

   

# Leer los datos de postgres y crear los csv