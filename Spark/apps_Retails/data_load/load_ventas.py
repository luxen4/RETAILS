import sesions
import psycopg2
from pyspark.sql import functions as F


def dropTable_ventas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """ DROP TABLE IF EXISTS ventas; """

        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'VENTAS' ELIMINADA successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)



def createTable_ventas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS ventas (
                registro_id SERIAL PRIMARY KEY,
                store_id INTEGER,
                store_name VARCHAR (100),
                location VARCHAR (100),
                date DATE,
                product_id INTEGER,
                product_name VARCHAR (100),
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



def insertar_Ventas(store_id,store_name,location,date,product_id,product_name,quantity_sold,revenue ):
    try:
        connection = psycopg2.connect(host="my_postgres_service", port="5432",database="retail_db", user="postgres", password="casa1234")
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO ventas (store_id,store_name,location,date,product_id,product_name,quantity_sold,revenue ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", 
                       (store_id,store_name,location,date,product_id,product_name,quantity_sold,revenue ))
                
        #connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/VENTAS.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)



def preparacion_df():
    try:
        
        spark=sesions.sesionSpark()
        
        bucket_name = 'my-local-bucket' 
        
        file_name='stores_csv'
        df_stores = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_stores.show()
        
        other_file_name ='sales_csv'
        df_sales = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        
        # Unir ambos DataFrames en función de la columna común store_ID
        df = df_stores.join(df_sales.select("store_id","date","product_id","quantity_sold","revenue"), "store_id", "left")
        df.show()
        
        
        file_name = 'products_json' 
        df_products= spark.read.json(f"s3a://{bucket_name}/{file_name}")
        df_products = df_products.withColumnRenamed("id", "product_id")
        df_products.show()
        df = df.join(df_products.select("product_id","tipo","talla","color"), "product_id", "left")
        df.show()
        
        
        # Eliminar columnas
        #df = df[[col for col in df.columns if col != "store_id"]]
        #df = df[[col for col in df.columns if col != "date"]]
        #df = df[[col for col in df.columns if col != "revenue"]]
           
           
           

        for row in df.select("*").collect():
            store_id = row.store_id
            store_name = row.store_name
            location = row.location
            date = row.date
            product_id=row.product_id
            product_name=row.tipo
            quantity_sold = row.quantity_sold
            revenue=row.revenue
      
            insertar_Ventas(store_id,store_name,location,date,product_id,product_name,quantity_sold,revenue)
        
        spark.stop()
    
    except Exception as e:
        print("error reading1 TXT")
        print(e)


dropTable_ventas()
createTable_ventas()
preparacion_df()



