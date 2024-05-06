from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F


def createTable_ventas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS ventas (
                registro_id SERIAL PRIMARY KEY,
                store_name VARCHAR (100),
                location VARCHAR (100),
                date DATE,
                product_id INTEGER,
                quantity_sold DECIMAL (10,2),
                revenue DECIMAL(10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'VENTAS' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)



def insertar_Ventas(store_id, store_name, date, product_ID, quantity_sold, revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO ventas (store_name, location, date, product_id, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s, %s);", 
                       (store_id, store_name, date, product_ID, quantity_sold, revenue))
                
        #connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/VENTAS.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


def preparacion_df():
    print("entro")
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
        file_name='data_stores.csv'
        other_file_name='sales_data.csv'
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_original.show()
        
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        
        # Unir ambos DataFrames en función de la columna común store_ID
        df = df_original.join(df_otro.select("store_id","date","product_id","quantity_sold","revenue"), "store_id", "left")
        df.show()
        
         # Eliminar columnas
        df = df[[col for col in df.columns if col != "store_id"]]
        df = df[[col for col in df.columns if col != "date"]]
        df = df[[col for col in df.columns if col != "revenue"]]
           

        for row in df.select("*").collect():
            store_ID = row.store_ID
            date = row.date
            store_name = row.store_name
            product_ID=row.product_ID
            quantity_sold = row.quantity_sold
            revenue=row.revenue
      
            insertar_Ventas(store_ID, store_name, date, product_ID, quantity_sold, revenue)
        
        spark.stop()
    
    except Exception as e:
        print("error reading1 TXT")
        print(e)

#df_final = juntarcolumnas()   
createTable_ventas()
preparacion_df()


# a. Análisis de ventas:  store_ID, store_name, ubicación, revenue
# ● ¿Qué tienda tiene los mayores ingresos totales?
# ● ¿Cuáles son los ingresos totales generados en una fecha concreta?
# ● ¿Qué producto tiene la mayor cantidad vendida?


# b. Análisis geográfico: store_ID, store_name, ubicación, revenue
# ● ¿Cuáles son las regiones con mejores resultados en función de los ingresos?
# ● ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?


# c. Análisis demográfico: store_ID, store_name, demographics, revenue
# ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?
# ● ¿Existen productos específicos preferidos por determinados grupos demográficos?


# d. Análisis temporal:   date,  ,revenue
# ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente)?
# ● ¿Existen tendencias estacionales en las ventas?


