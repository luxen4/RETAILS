from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F
import sesionspark


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
        spark = sesionspark.sessionSpark2()
        
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


