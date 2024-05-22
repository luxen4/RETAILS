# Lea de mysql, y aloge csv en S3 y local (podría venir desde un .txt la inserción en mysql)

from pyspark.sql import SparkSession
import mysql.connector


def selectTable():
    try:
        conexion = mysql.connector.connect( host="mysql", user="user1",password="alberite", database="retail_db")
        cursor = conexion.cursor()

        sql = """Select * from clients; """

        cursor.execute(sql)
        resultados = cursor.fetchall()    # Obtener los resultados
        
        print("Datos OK.")
        return resultados
        
    except Exception as e:
        print(e)
        #print(f"File '{filename}' not found.")
        print("No se ha podido leer de la tabla.")


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
    .config("spark.jars","./postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()
    
    
    resultados = selectTable()
    data=[]
    for fila in resultados:
        print(fila)  # Aquí puedes hacer algo con cada fila
        
        linea={'style':fila[0],'marca':fila[1],'model':fila[2],'years':fila[3],'precio':fila[3]}
        data.append(linea)
        
    df = spark.createDataFrame(data, ["client_name", "edad", "apellidos", "dni"])
        
    
    # csv
    ruta_salida = "s3a://my-local-bucket/mysqlCliente_csv"
    df = df.write.csv(ruta_salida, mode="overwrite")
    
    # Leer el csv
    bucket_name = 'my-local-bucket' 
    file_name='mysqlCliente_csv'
    df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
    df_original.show()

    
    # json
    df = spark.createDataFrame(data)
    ruta_salida = "s3a://my-local-bucket/mysqlCliente_json"
    df = df.write.option("multiline", "true").json(ruta_salida, mode="overwrite" )
    
    
    
    # Leer el json
    bucket_name = 'my-local-bucket'
    file_name='mysqlCliente_json'
    df_original = spark.read.json(f"s3a://{bucket_name}/{file_name}")
    df_original.show()

    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

