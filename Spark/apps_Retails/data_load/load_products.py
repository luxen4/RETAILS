from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F
import sesionspark

def insertar_Products(product_ID, quantity_sold, demographics, tipo):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO products (product_ID, quantity_sold, demographics, tipo) VALUES (%s, %s, %s, %s);", 
                       (product_ID, quantity_sold, demographics, tipo))
                
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/products.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


def preparacion_df():
    print("entro")
    try:
        spark = sesionspark.sessionSpark2()
        
        
        # Unir ambos DataFrames en función de la columna común store_ID
        bucket_name = 'my-local-bucket' 
        file_name='sales_data.csv'
        other_file_name='data_stores.csv'
        
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        df = df_original.join(df_otro.select("store_ID", "demographics"), "store_ID", "left")
        df.show()
            
            
        bucket_name = 'my-local-bucket' 
        other_file_name='data_products.csv'
        
        df_otro = spark.read.csv(f"s3a://{bucket_name}/{other_file_name}", header=True, inferSchema=True)
        df = df.join(df_otro.select("product_ID", "tipo"), "product_ID", "left")
        
        
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "store_ID"]]
        df = df[[col for col in df.columns if col != "date"]]
        df = df[[col for col in df.columns if col != "revenue"]]
        df.show()
        
            
        for row in df.select("*").collect():
            product_ID=row.product_ID
            quantity_sold = row.quantity_sold
            demographics = row.demographics
            tipo = row.tipo
            
            insertar_Products(product_ID, quantity_sold, demographics, tipo)
        
        spark.stop()
    
    except Exception as e:
        print("error reading1 TXT")
        print(e)

#df_final = juntarcolumnas()   
preparacion_df()
        
