from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, when, col, mean 
from pyspark.sql.functions import col, year, month, dayofmonth , lit, concat, substring, to_date
from datetime import datetime

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
    
try:
    #date,store_ID,product_ID,quantity_sold,revenue
    df_filtrado = spark.read.csv("s3a://my-local-bucket/sales_data.csv", header=True)  # Leer el archivo CSV

    # Fechas van a año/mes/dia
    columna_fecha = "date"
    df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%Y-%m-%d")})        # Nulos a fecha actual
    df_filtrado = df_filtrado.withColumn(columna_fecha, to_date(col(columna_fecha), "yyyy-M-d"))     # Castear a Date
    df_filtrado = df_filtrado.withColumn("Año", year(columna_fecha))
    df_filtrado = df_filtrado.withColumn("Mes", month(columna_fecha))
    df_filtrado = df_filtrado.withColumn("Dia", dayofmonth (columna_fecha))
    df_filtrado = df_filtrado.withColumn("Mes/Año", concat(month(columna_fecha), lit("-"), year(columna_fecha)))
        
    # Nulos por otro valor
    columna_especifica = "store_ID"
    valor_reemplazo = 0
    df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

    # Sustituir los valores vacios con un 0
    columna_especifica = "product_ID"
    valor_reemplazo = 0
    df_sustituido = df_filtrado.na.fill({columna_especifica: valor_reemplazo})


    # Sustituir los valores vacios con la media
    columna_especifica = "quantity_sold"
    mean_quantity_sold = df_filtrado.select(mean(col(columna_especifica))).collect()[0][0]
    mean_quantity_sold = round(mean_quantity_sold,2)
    df_filtrado = df_filtrado.withColumn(columna_especifica, 
                                        when(col(columna_especifica).isNull(), mean_quantity_sold).otherwise(col(columna_especifica)))
    
    # Sustituir los valores vacios con la media
    columna_especifica = "revenue"
    mean_quantity_sold = df_filtrado.select(mean(col(columna_especifica))).collect()[0][0]
    mean_quantity_sold = round(mean_quantity_sold,2)
    
    
    
    ######## ZA  #########
    '''
    # Reemplazar los valores no numéricos con la media
    df_filtrado = df_filtrado.withColumn(columna_especifica, 
                                     when(col(columna_especifica).cast("float").isNull(), mean_quantity_sold)
                                     .otherwise(when(col(columna_especifica).cast("float").isNaN(), mean_quantity_sold)
                                                .otherwise(col(columna_especifica))))
    
    
    df_filtrado = df_filtrado.withColumn(columna_especifica, 
                                        when(col(columna_especifica).isNull(), mean_quantity_sold).otherwise(col(columna_especifica)))
    '''
    
    # Reemplazar los valores no numéricos en la columna "revenue" con la media
    media_revenue = df_filtrado.select(mean(columna_especifica)).collect()[0][0]
    df_filtrado = df_filtrado.withColumn(columna_especifica,
                                when(col(columna_especifica).cast("float").isNotNull(),
                                    col(columna_especifica)).otherwise(round(media_revenue,2)))
    
    ##########
    # Agregar columna "fecha_procesado" y "nivel_procesado" con la fecha actual
    df_filtrado = df_filtrado.withColumn("fecha_procesado", lit(datetime.now().strftime("%Y-%m-%d")))
    df_filtrado = df_filtrado.withColumn("nivel_procesado", lit(1))
    ##########
    
    
    # Escribe el DataFrame como un archivo CSV localmente
    output_file_path = "s3a://my-local-bucket/sales_data_procesado.csv"
    df_filtrado.write.csv(output_file_path, mode="overwrite", header=True)

    df_filtrado.show()
    spark.stop()

except Exception as e:
    print("error reading TXT")
    print(e)
    

# Reemplazar valores vacíos en la columna "Nombre" con "Adri"
#df = df_filtrado.withColumn("Apellido1", when(col("Apellido1") == 'Laya', "Biri").otherwise(col("Apellido1")))

# Reemplazar valores vacíos en la columna "Nombre" con "Adri"
#df = df_filtrado.fillna({'Nombre': 'Adri'})

# Filtrar los registros donde "product_ID" no es un número
# df_filtrado = df_sustituido.filter(df_sustituido["product_ID"].cast("int").isNotNull())


# Reemplazar los valores no numéricos con "AAAA"
'''
df_filtrado = df_filtrado.withColumn(columna_especifica,
when(col(columna_especifica).cast("float").isNotNull(),
col(columna_especifica)).otherwise("AAAA"))'''




####
# Procesa un .csv de un bucket, 
# Utiliza Spark para el procesado de columnas.
# Devuelve otro .csv al bucquel con el nivel y la fecha de procesado    
####
    