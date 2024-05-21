from pymongo import MongoClient
import json, csv


from pyspark.sql import SparkSession

def sesionSpark():
    
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
    .master("local[*]") \
    .getOrCreate()
    
    return spark




# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        for item in data:
            print(item)
            # Eliminar la línea que convierte la fecha a una cadena ISO
            # item['Fecha'] = item['Fecha'].isoformat()
            json.dump(item, file)
            file.write('\n')
    print(f"File '{filename}' created successfully!")
  
  
# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    products = data[0]['productos']
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["product_ID", "tipo", "talla", "color"])  # Escribir el encabezado
        for product in products:
            writer.writerow([product['id'], product['tipo'], product['talla'], product['color']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")



client = MongoClient()                      # Accede a la colección "ropa"
db = client["products"]
ropa_collection = db["ropa"] 


resultados = ropa_collection.find()         # Obtener los resultados de la colección
productos_list = list(resultados)

# Convertir los ObjectId a cadenas en cada documento
for producto in productos_list:
    print(producto)
    producto['_id'] = str(producto['_id'])


# csv
spark=sesionSpark()
df = spark.createDataFrame(productos_list)
ruta_salida = "s3a://my-local-bucket/products_csv"
df = df.write.csv(ruta_salida, header=True, mode="overwrite")




# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
#consulta = { "tipo": "pantalones" }
# Ejecuta la consulta y obtén los resultados
#resultados = ropa_collection.find(consulta)
