from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F

def insertar_CargosSumaSalario(cargo, total_salario):
    try:
        connection = psycopg2.connect(host='my_postgres_service', port='5432',database='warehouse_retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  

        cursor.execute("INSERT INTO cargosalario (cargo, salario) VALUES (%s, %s);", 
                       (cargo, total_salario))
                
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Warehouse/cargosalario.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


def agrupacion_df():
    
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
        .master("local[*]") \
        .getOrCreate()

        bucket_name = 'my-local-bucket' 
        # OKfile_name='sales_data.csv'
        file_name='data_pokemon.csv'
        
        df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df.show()
        
        # Agrupar por store_ID y sumar el revenue
        df_grouped = df.groupBy("cargo").agg(F.sum("salario").alias("total_salario"))
        df_grouped.show()
        
        # Mostrar los valores de las columnas
        
        for row in df_grouped.select("*").collect():
            print (row)
            cargo = row.cargo
            total_salario = row.total_salario
            print(f"Cargo: {cargo}, total_salario: {total_salario}")
            insertar_CargosSumaSalario(cargo, total_salario)
        spark.stop()
    
    except Exception as e:
        print("error reading1 TXT")
        print(e)

#df_final = juntarcolumnas()   
agrupacion_df()
