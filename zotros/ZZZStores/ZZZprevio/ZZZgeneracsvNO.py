from pyspark.sql import SparkSession

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
.getOrCreate()

def creaStoresJDBC():
    try:
        
        # Define connection properties
        jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
        connection_properties = {"user": "postgres", "password": "casa1234","driver": "org.postgresql.Driver"}

        
        # Read data from PostgreSQL table into a DataFrame
        df = spark.read.jdbc(url=jdbc_url, table="stores", properties=connection_properties)
        df.show()
        
        # Reemplazar nulos y vacíos
        columna_especifica = "store_name"
        valor_reemplazo = "?"
        df = df.na.replace('', valor_reemplazo, columna_especifica).na.fill({columna_especifica: valor_reemplazo})

        # Sustituir vacios con un 0
        columna_especifica = "location"
        valor_reemplazo = "?"
        df = df.na.fill({columna_especifica: valor_reemplazo})
        
        # Reemplazar nulos y vacíos 
        columna_especifica = "demographics"
        valor_reemplazo = "???"
        df = df.na.replace('', valor_reemplazo, columna_especifica).na.fill({columna_especifica: valor_reemplazo})
        df.show()
        
        # Guardar el DataFrame como un archivo CSV
        df.write.csv("./../../1_data_bda/csv/stores.csv", header=True, mode="overwrite")
        
        #df.write.csv("s3a://my-local-bucket/stores_exportados2.csv", header=True, mode="overwrite")
        
    except Exception as e:
        print("Error reading data from PostgreSQL:", e)

    finally:
        # Stop SparkSession
        spark.stop()
       
if __name__ == "__main__":
    creaStoresJDBC()
    
    
    
    
    
    
    
    
    
### WAREHOUSE  
######### Escribir el DataFrame en la tabla de PostgreSQL
#def aWarehouse(df): 
    #jdbc_url = "jdbc:postgresql://spark-database-1:5432/warehouse_retail_db"
    #df.write.jdbc(url=jdbc_url, table="stores", mode="overwrite", properties=connection_properties)

