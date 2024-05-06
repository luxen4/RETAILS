from pyspark.sql import SparkSession

def select(store_name):
    # Iniciar sesión de Spark
    spark = SparkSession.builder \
        .appName("ReadFromPostgres") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .master("spark://spark-master:7077") \
        .config("spark.jars","postgresql-42.7.3.jar") \
        .getOrCreate()
        
    # Definir propiedades de conexión
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="stores", properties=connection_properties)

    df.createOrReplaceTempView("tabla_spark")

    resultado = spark.sql("SELECT * FROM tabla_spark WHERE store_name ='" + store_name + "';")
    resultado.show()

    # Procesar resultados
    store_id = None
    if not resultado.isEmpty():
        row = resultado.collect()[0]
        store_id = row["store_id"]
        store_name = row["store_name"]
        location = row["location"]
        demographics = row["demographics"]
        
        print("store_id:", store_id)
        print("store_name:", store_name)
        print("location:", location)
        print("demographics:", demographics)
        print()

    spark.stop()
    
    return store_id

select('OKG')