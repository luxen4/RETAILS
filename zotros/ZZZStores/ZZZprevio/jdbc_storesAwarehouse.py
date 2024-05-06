from pyspark.sql import SparkSession


def read_from_postgres():
    spark = SparkSession.builder \
        .appName("ReadFromPostgres") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .master("spark://spark-master:7077") \
        .config("spark.jars","postgresql-42.7.3.jar") \
        .getOrCreate()

    # Define connection properties
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = {"user": "postgres", "password": "casa1234","driver": "org.postgresql.Driver"}

    try:
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
        
        
        
        
        
        
        
        
        
### WAREHOUSE        
######### Escribir el DataFrame en la tabla de PostgreSQL
        spark = SparkSession.builder \
        .appName("ReadFromPostgres") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .master("spark://spark-master:7077") \
        .config("spark.jars","postgresql-42.7.3.jar") \
        .getOrCreate()


        #connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}
        jdbc_url = "jdbc:postgresql://spark-database-1:5432/warehouse_retail_db"
        df.write.jdbc(url=jdbc_url, table="stores", mode="overwrite", properties=connection_properties)
##########

    except Exception as e:
        print("Error reading data from PostgreSQL:", e)

    finally:
        # Stop SparkSession
        spark.stop()
       
if __name__ == "__main__":
    read_from_postgres()
    
    
# Nota: Este archivo extrae de postgress, procesa y mete en la parte de WAREHOUSE

# docker exec -it 03c14ab5a86c23cb866130f502469fcb7113526f0092c91b300207617f566587 /bin/bash     # contenedor spark-spark-master-1
# cd /opt/spark-apps
# python prueba.py
    
    
    
    
    
    
    
    
    
    
    

# Debe haber en la base de datos los registros cargados(con errores) en la tabla stores

# Nota: Este archivo extrae de postgress, procesa y mete en la parte de WAREHOUSE

# Antes de ejecutar, crear en WAREHOUSE la tabla stores con el archivo createStores.py
