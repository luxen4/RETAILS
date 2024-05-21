# Lea de mysql, y aloge csv en S3
from pyspark.sql import SparkSession
from pymongo import MongoClient


# client = MongoClient("mongodb://root:secret@localhost:27017/")   # Clase               # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
client = MongoClient("mongodb://spark-mongodb-1:27017/") 

db = client["products"]
ropa_collection = db["ropa"]  # Accede a la colección "ropa"

# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
consulta = { "tipo": "pantalones" }

# Ejecuta la consulta y obtén los resultados
#resultados = ropa_collection.find(consulta)
resultados = ropa_collection.find()

# Imprime los resultados
print("Productos encontrados:")

for producto in resultados:
    print(producto)
    
    print("ID del documento:", producto["_id"])
    lista_productos = producto["productos"]
    print("Productos en este documento:")
    
    for p in lista_productos:
        print("ID:", p["id"])
        print("Tipo:", p["tipo"])
        print("Talla:", p["talla"])
        print("Color:", p["color"])
        print()  # Agrega una línea en blanco entre cada producto





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
    
    

    data=[]
    for fila in resultados:
        print(fila)  
        linea={'style':fila[0],'marca':fila[1],'model':fila[2],'years':fila[3],'precio':fila[3]}
        data.append(linea)
        
    df = spark.createDataFrame(data, ["client_name", "edad", "apellidos", "dni"])
        
    
    # csv
    ruta_salida = "s3a://my-local-bucket/mysqlCliente_csv"
    df = df.write.csv(ruta_salida, mode="overwrite")

    
    # json
    df = spark.createDataFrame(data)
    ruta_salida = "s3a://my-local-bucket/mysqlCliente_json"
    df = df.write.option("multiline", "true").json(ruta_salida, mode="overwrite" )

    
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)

