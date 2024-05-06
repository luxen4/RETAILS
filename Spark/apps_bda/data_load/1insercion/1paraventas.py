from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F
import sesionspark




def insertar_Ventas(store_id, store_name, date, product_ID, quantity_sold, revenue):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO ventas (store_id, store_name, date, product_ID, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s, %s);", 
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
        
        spark = sesionspark.sessionSpark2()
        bucket_name = 'my-local-bucket' 
        file_name='data_stores.csv'
        other_file_name='sales_data.csv'
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_original.show()
        
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        
        # Unir ambos DataFrames en función de la columna común store_ID
        df = df_original.join(df_otro.select("store_ID","date","product_ID","quantity_sold","revenue"), "store_ID", "left")
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
preparacion_df()
