import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Crear una tabla para responder a las preguntas de ANALISIS-DEMOGRAFICO en WAREHOSE
def createTable_demografico():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
                    CREATE TABLE IF NOT EXISTS demografico (
                    demografico_ID SERIAL PRIMARY KEY,
                    store_id INTEGER,
                    store_name VARCHAR (100),
                    demographics VARCHAR (100),
                    revenue DECIMAL(10,2)
                    );
                """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'DEMOGRAFICO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)



def insertar_demografico(demographics, revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service' , port='5432',database='warehouse_retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO demografico (demographics, revenue) VALUES (%s, %s);", 
                       (demographics, revenue))
                
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/Demographics.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
    

def juntarcolumnas():
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
        
        # Unir ambos DataFrames en función de la columna común store_ID
        bucket_name = 'my-local-bucket' 
        file_name='data_stores.csv'
        other_file_name='sales_data.csv'
       
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        df = df_original.join(df_otro.select("store_ID", "date", "product_ID", "quantity_sold","revenue"), "store_ID", "left")

        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "store_id"]]
        df = df[[col for col in df.columns if col != "store_name"]]
        df = df[[col for col in df.columns if col != "date"]]
        df = df[[col for col in df.columns if col != "product_ID"]]
        df = df[[col for col in df.columns if col != "location"]]
        df = df[[col for col in df.columns if col != "quantity_sold"]]
        
        # Agrupar por store_ID y sumar el revenue
        df_grouped = df.groupBy("demographics").agg(F.sum("revenue").alias("total_revenue"))
        df_grouped.show()
        
         # Mostrar los valores de las columnas
        for row in df_grouped.select("*").collect():
            demographics = row.demographics
            total_revenue = row.total_revenue
            print(f"Demographics: {demographics}, revenue: {total_revenue}")
            insertar_demografico(demographics, total_revenue)
   
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

df = juntarcolumnas()   

# c. Análisis demográfico: store_ID, store_name, demographics, revenue
# ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?
# ● ¿Existen productos específicos preferidos por determinados grupos demográficos?
