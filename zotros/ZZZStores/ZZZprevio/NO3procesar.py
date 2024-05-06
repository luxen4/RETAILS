
from pyspark.sql import SparkSession




def read_from_postgres():
    try:
       
        '''
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
        .config("spark.jars","postgresql-42.7.3.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()'''
        
        
        
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
        
    
        
        jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
        connection_properties = { "user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

        table_name = "stores"   # Define table name
        df_filtrado = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        df_filtrado.show()
        
        # Reemplazar nulos y vacios"
        columna_especifica = "store_name"
        valor_reemplazo = "?"
        df_filtrado = df_filtrado.na.replace('', valor_reemplazo, columna_especifica).na.fill({columna_especifica: valor_reemplazo})

        columna_especifica = "location"
        valor_reemplazo = "?"
        df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})
        
        columna_especifica = "demographics"
        valor_reemplazo = "?"
        df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

        df_filtrado.show()
        
        
        # Formar el csv
        output_file_path = "s3a://my-local-bucket/stores_data_procesado.csv"
        df_filtrado.write.csv(output_file_path, mode="overwrite", header=True)
        
            
    except Exception as e:
        print("Error reading data from PostgreSQL:", e)

    finally:
        # Stop SparkSession
        spark.stop()
        
read_from_postgres()
    
    
    
        
        
#'''
# POR LA MEDIA"
#columna_especifica = "demografics"
#valor_reemplazo = "???"
#df_filtrado = df_filtrado.na.replace('', valor_reemplazo, columna_especifica).na.fill({columna_especifica: valor_reemplazo})

    
    
# Nota: Este archivo extrae de postgress, procesa y mete en la parte de WAREHOUSE

# docker exec -it 03c14ab5a86c23cb866130f502469fcb7113526f0092c91b300207617f566587 /bin/bash     # contenedor spark-spark-master-1
# cd /opt/spark-apps
# python prueba.py