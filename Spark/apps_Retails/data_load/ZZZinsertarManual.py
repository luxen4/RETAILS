from pyspark.sql import SparkSession
import psycopg2, random

def insertar_ventas(store_id, store_name, location, dates, product_id, quantity_sold, revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service' , port='5432',database='warehouse_retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  
        # ventas_id | date | store_id | store_name | product_id | quantity_sold | revenue
        
        # Luego puedes usar formatted_date en tu consulta SQL
        cursor.execute("INSERT INTO ventas (store_id, store_name, location, date, product_id, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s);", 
                       (store_id, store_name, location, dates, product_id, quantity_sold, revenue))
                
        # cursor.execute("INSERT INTO ventas (store_id, store_name, location, date, product_id, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s);",(store_id, store_name, location, dates, product_id, quantity_sold, revenue))
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/Ventas.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
    


def insertarmanual():
    
    spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.7.3.jar") \
    .getOrCreate()

    bucket_name = 'my-local-bucket' 
    file_name='stores_procesados.csv/'
    
    df_final = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)

    # Imprime el esquema del DataFrame
    df_final.printSchema()
    df_final.show()

    # Utiliza el DataFrame directamente para insertar en la base de datos
    
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/warehouse_retail_db"
    connection_properties = { "user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}
    table_name="ventas"
    df_final.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)
        
    
    for index in range(len(df_final)):
        date = df_final[index]['date']
        store_id = df_final[index]['store_ID']
        store_name = df_final[index]['store_name']
        product_id = df_final[index]['product_ID']
        quantity_sold = df_final[index]['quantity_sold']
        revenue = df_final[index]['revenue']
        
        # Aquí puedes hacer lo que necesites con los valores de cada fila, por ejemplo, imprimirlos
        print(f"Date: {date}, Store ID: {store_id}, Store Name: {store_name}, Product ID: {product_id}, Quantity Sold: {quantity_sold}, Revenue: {revenue}")

        insertar_ventas(date, store_id, store_name, product_id, quantity_sold, revenue)
        
        
insertarmanual()