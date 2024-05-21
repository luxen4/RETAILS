from pyspark.sql import SparkSession
#import psycopg2
import prueba

#connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')

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
        store_name = df.select("store_name").collect()[0][0]
        
        valor_reemplazo = "???"
        df = df.na.replace('', valor_reemplazo, columna_especifica).na.fill({columna_especifica: valor_reemplazo})

        # Sustituir vacios con un 0
        columna_especifica = "location"
        valor_reemplazo = "???"
        df = df.na.fill({columna_especifica: valor_reemplazo})
        
        # Reemplazar nulos y vacíos 
        columna_especifica = "demographics"
        valor_reemplazo = "???"
        df = df.na.replace('', valor_reemplazo, columna_especifica).na.fill({columna_especifica: valor_reemplazo})
        df.show()
        
    except Exception as e:
        print("Error reading data from PostgreSQL:", e)

    finally:
        # Stop SparkSession
        spark.stop()
      

if __name__ == "__main__":
    read_from_postgres()

