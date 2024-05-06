from pyspark.sql import SparkSession
#import sesionspark

def leerProcesarConSpark():
    
    try:
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
        .master("local[*]") \
        .getOrCreate()

        bucket_name = 'my-local-bucket' 
        file_name='stores_exportados.csv'
        other_file_name='sales_data.csv'
        
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)

        # Unir ambos DataFrames en función de la columna común store_ID
        df_final = df_original.join(df_otro.select("store_ID", "revenue"), "store_ID", "left")
        df_final.show()

        # Puedes agregar más operaciones de procesamiento aquí
        
        
        df_final \
        .write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
        .mode('overwrite') \
        .csv(path='s3a://my-local-bucket/stores_procesado.csv', sep=',')
        
        spark.stop()
    
    except Exception as e:
        print("error reading3 TXT")
        print(e)