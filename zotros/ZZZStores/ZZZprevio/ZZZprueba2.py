from pyspark.sql import SparkSession

def prueba(df_final):

    # Configura la sesión de Spark
    spark = SparkSession.builder \
        .appName("InsertIntoPostgres") \
        .config("spark.driver.extraClassPath", "postgresql-42.7.3.jar") \
        .master("local[*]") \
        .getOrCreate()

    # Lee los datos del DataFrame de Spark
    data = [
    (1, "VSF", "Santander", None, 3, 21, 1619),
    (1, "VSF", "Santander", "2018-05-21", 9, 3, 4003),
    (1, "VSF", "Santander", None, 3, 100, 4564),
    (1, "VSF", "Santander", "2000-01-29", 1, None, 2175),
    (1, "VSF", "Santander", "2019-10-10", 9, 73, None)
]
    
    columns = ["store_id", "store_name", "location", "date", "product_ID", "quantity_sold", "revenue"]
    df = spark.createDataFrame(data, columns)
    
    # Convierte la columna 'date' a tipo de dato 'date'
    df = df.withColumn("date", to_date(df["date"]))

    # Escribe los datos en la tabla PostgreSQL
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/warehouse_retail_db"
    connection_properties = {
        "user": "postgres",
        "password": "casa1234",
        "driver": "org.postgresql.Driver"
    }
    table_name = "ventas"

    #df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)

    # Detén la sesión de Spark
    spark.stop()
   
