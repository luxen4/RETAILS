
# create database retail_db;
import psycopg2

def drop_Table():
    
    try:
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
   
        create_table_query = """ DROP TABLE IF EXISTS stores; """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'STORES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


     
def create_Table():
    try:
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
   
        create_table_query = """ CREATE TABLE IF NOT EXISTS stores(
                                    store_id SERIAL PRIMARY KEY,
                                    store_name VARCHAR(100),
                                    location VARCHAR (100),
                                    demographics VARCHAR (100)
                                )
                                """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'STORES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)





from pyspark.sql import SparkSession


def insertarPostgres():
    spark = SparkSession.builder \
    .appName("ReadFromPostgres") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("spark://spark-master:7077") \
    .config("spark.jars","postgresql-42.7.3.jar") \
    .getOrCreate()

    df_original = spark.read.csv("/opt/spark-data/csv/data_stores.csv", header=True)
    df_original.show()
    
    
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = { "user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    table_name = "stores" 
    # Escribe el DataFrame en la tabla de PostgreSQL
    df_original.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties) # mode="append"
    



drop_Table()
create_Table()
insertarPostgres()






# Agregar una tupla a la lista
#data.append((store_id, store_name, location, date, product_id, quantity_sold, revenue))
'''
columns = ["store_id", "store_name", "location", "date", "product_ID", "quantity_sold", "revenue"]
df = spark.createDataFrame(data, columns)

# Convierte la columna 'date' a tipo de dato 'date'
df = df.withColumn("date", to_date(df["date"]))
df.show()'''
    















# Agregar una tupla a la lista
#data.append((store_id, store_name, location, date, product_id, quantity_sold, revenue))
'''
columns = ["store_id", "store_name", "location", "date", "product_ID", "quantity_sold", "revenue"]
df = spark.createDataFrame(data, columns)

# Convierte la columna 'date' a tipo de dato 'date'
df = df.withColumn("date", to_date(df["date"]))
df.show()'''
    


# email VARCHAR(255) NOT NULL,
# bird_date DATE
# created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP













