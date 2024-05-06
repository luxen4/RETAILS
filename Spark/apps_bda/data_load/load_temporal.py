from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F


# Crear una tabla para responder a las preguntas de ANALISIS-TEMPORAL en WAREHOSE
def createTable_temporal():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
                    CREATE TABLE IF NOT EXISTS temporal (
                    temporal_ID SERIAL PRIMARY KEY,
                    date DATE,
                    revenue DECIMAL(10,2)
                );
                """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'TEMPORAL' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


def insertar_temporal(dates,revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db' , user= 'postgres', password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO temporal (date, revenue) VALUES (%s, %s);", (dates, revenue))
                
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/Temporal.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
    

def preparar_df():
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
        
        # Crear un nuevo DataFrame desde 2"
        bucket_name = 'my-local-bucket' 
        other_file_name='sales_data.csv'
        df = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        
        # Eliminar columnas
        df = df[[col for col in df.columns if col != "demographics"]]
        df = df[[col for col in df.columns if col != "store_ID"]]
        df = df[[col for col in df.columns if col != "product_ID"]]
        df = df[[col for col in df.columns if col != "quantity_sold"]]
        df = df[[col for col in df.columns if col != "Año"]]
        df = df[[col for col in df.columns if col != "Mes"]]
        df = df[[col for col in df.columns if col != "Dia"]]
        df = df[[col for col in df.columns if col != "Mes/Año"]]
        df = df[[col for col in df.columns if col != "fecha_procesado"]]
        df = df[[col for col in df.columns if col != "nivel_procesado"]]
        
        # Agrupar por date y sumar el revenue
        df_grouped = df.groupBy("date").agg(F.sum("revenue").alias("total_revenue"))
        df_grouped.show()

        
        # Mostrar los valores de las columnas
        for row in df_grouped.select("*").collect():
            print(row)
            date=row.date
            total_revenue=row.total_revenue
            #print(f"date: {date}, revenue: {revenue}")
            insertar_temporal(date, total_revenue)
   
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


preparar_df()   


# d. Análisis temporal:   date,  ,revenue
# ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente)?
# ● ¿Existen tendencias estacionales en las ventas?


