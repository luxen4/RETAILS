from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F


# Crear una tabla para responder a las preguntas de ANALISIS-GEOGRAFICO en WAREHOSE
def createTable_geografico():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS geografico (
                geografico_ID SERIAL PRIMARY KEY,
                store_id INTEGER,
                store_name VARCHAR (100),
                ubicacion VARCHAR (100),
                revenue DECIMAL(10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'GEOGRAFICO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)




def insertar_geografico(ubicacion,revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db' , user= 'postgres', password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO geografico (location, revenue) VALUES (%s, %s);", (ubicacion, revenue))
                
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/Geografico.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
    

def formar_df():
    
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
    
    try:
         # Unir ambos DataFrames en función de la columna común store_ID
        bucket_name = 'my-local-bucket' 
        file_name='data_stores.csv'
        other_file_name='sales_data.csv'
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        df = df_original.join(df_otro.select("store_ID","revenue"), "store_ID", "left")


        # Eliminar columnas
        df = df[[col for col in df.columns if col != "store_id"]]
        df = df[[col for col in df.columns if col != "store_name"]]
        df = df[[col for col in df.columns if col != "demographics"]]
        df.show()
        
        # Agrupar por store_ID y sumar el revenue
        df_grouped = df.groupBy("location").agg(F.sum("revenue").alias("total_revenue"))
        df_grouped.show()
        
        for row in df_grouped.select("*").collect():
            location = row.location
            total_revenue = row.total_revenue
            print(f"Location: {location}, revenue: {total_revenue}")
            
            insertar_geografico(location, total_revenue)
        
        spark.stop()
        
    except Exception as e:
        print("error reading TXT")
        print(e)

formar_df()   



# warehouse_retail_db=# select * from geografico;
# geografico_id | store_id | store_name | ubicacion | revenue

# 2 b. Análisis geográfico: ubicación, revenue
# ● ¿Cuáles son las regiones con mejores resultados en función de los ingresos?
# ● ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?
