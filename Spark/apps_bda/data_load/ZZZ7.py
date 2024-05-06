import psycopg2
import sesionspark

def insertar_ventas( store_id, store_name, location, date, product_id, quantity_sold, revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db', user= 'postgres', password='casa1234' )
        cursor = connection.cursor()  
        cursor.execute("INSERT INTO ventas (store_id, store_name, location, date, product_id, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s);",(date, store_id, store_name, location, product_id, quantity_sold, revenue))
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/Ventas.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
    

def leerCSV_insertar():
    try:
        spark = sesionspark.sessionSpark2()
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()
        
         # Mostrar los valores de las columnas
        for row in df.select("*").collect():
            store_id= row[0]
            store_name = row[1]
            location= row[2]
            date = row[3]
            product_id= row[4]
            quantity_sold = row[5]
            revenue = row[6]

            # Imprimir los valores de las columnas
            print(f"store_id: {store_id}, store_name: {store_name}, location: {location}, date: {date}, product_id: {product_id}, quantity_sold: {quantity_sold}, revenue: {revenue}")
            insertar_ventas(store_id, store_name, location, date, product_id, quantity_sold, revenue)
    
    except Exception as e:
        print("Error de algún tipo:", e)


def leerCSVbucket_guardarS3(bucket_name, file_name):
    try:
        spark = sesionspark.sessionSpark2()
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()

        # Convertir el DataFrame en un archivo CSV y guardarlo en S3
        try:
            df \
            .select("store_id", "store_name", "location", "date", "product_id", "quantity_sold", "revenue") \
            .write \
            .option('header', 'true') \
            .option("delimiter", ",") \
            .mode('overwrite') \
            .csv("s3a://my-local-bucket/output")
            
            print("Archivo CSV guardado en S3 exitosamente.")

        except Exception as e:
            print("Error al guardar el archivo CSV en S3:", e)

        spark.stop()
        
    except Exception as e:
        print("error reading TXT")
        print(e)
            


def leeroutput_insertarJDBC(bucket_name, file_name):
    try:
        spark = sesionspark.sessionSpark2()
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()
        
        spark = sesionspark.sessionSparkJdbc() 
        
        # Define connection properties
        jdbc_url = "jdbc:postgresql://spark-database-1:5432/warehouse_retail_db"
        connection_properties = { "user": "postgres","password": "casa1234","driver": "org.postgresql.Driver"}
        df.write.jdbc(url=jdbc_url, table="ventas", mode="overwrite", properties=connection_properties)

    except Exception as e:
        print("error reading TXT")
        print(e)
    

bucket_name = 'my-local-bucket' 
file_name='stores_procesados.csv'
df_original=leerCSVbucket_guardarS3(bucket_name, file_name)


bucket_name = 'my-local-bucket' 
file_name='output/'
leeroutput_insertarJDBC(bucket_name, file_name)