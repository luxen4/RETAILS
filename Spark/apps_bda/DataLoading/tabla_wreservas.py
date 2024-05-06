import psycopg2
from pyspark.sql import SparkSession
# Crear una tablas para responder a las preguntas de ANALISIS



# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wReservas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_reservas (
                id_registro SERIAL PRIMARY KEY,
                id_reserva VARCHAR (100),
                fecha_entrada DATE,
                fecha_salida DATE,
                nombre_hotel VARCHAR (100),
                empleados VARCHAR (500),
                categoria_habitacion VARCHAR (100),
                tarifa_nocturna Decimal(10,2),
                preferencia_comida VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  



def insertarTable_wreservas(id_reserva, fecha_entrada, fecha_salida, nombre_hotel, empleados):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_reservas (id_reserva, fecha_entrada, fecha_salida, nombre_hotel, empleados) VALUES (%s, %s, %s, %s, %s);", 
                       (id_reserva, fecha_entrada, fecha_salida, nombre_hotel, empleados))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_reservas.")
     
     
     
def dataframe_wreservas():
    
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

    try:
        
        bucket_name = 'my-local-bucket'
        file_name = 'data_reservas' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_reservas.show()
        
        
        bucket_name = 'my-local-bucket' 
        file_name='restaurantes.json'      
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_restaurantes.show()
        
        
        df = df_reservas.join(df_restaurantes.select("id_restaurante","nombre","id_hotel"), "id_restaurante", "left")  #### METER UN NOMBRE DE MENU
        df = df.withColumnRenamed("nombre", "restaurante_name")                                             # Cambiar el nombre de la columna
        #df.show()
        
        
        bucket_name = 'my-local-bucket' 
        file_name = 'data_hoteles.json'
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_hoteles.show()
        
        
        df = df.join(df_hoteles.select("id_hotel","nombre_hotel","empleados"), "id_hotel", "left")
        df.show()
        
        
        
        bucket_name = 'my-local-bucket'
        file_name = 'data_habitaciones.csv' 
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_habitaciones.show()
        
        df = df.join(df_habitaciones.select("id_hotel","categoria"), "id_hotel", "left")
        df.show()
        
        
        #CLIENTES
        
        
        
        
        
        
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_restaurante"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
        # Mostrar el DataFrame resultante
        df.show()
        
        # df = df.dropDuplicates()    # Eliminar registros duplicados

       
        
        # No tocar que es OK
        for row in df.select("*").collect():
            print(row)
            id_reserva=row["id_reserva"]
            fecha_llegada=row["fecha_llegada"]
            fecha_salida=row["fecha_salida"]
            nombre_hotel=row["nombre_hotel"]
            empleados=row["empleados"]
            
            print(f"""
                  id_reserva-Cliente: {id_reserva}, 
                  fecha_llegada: {fecha_llegada},
                  fecha_salida: {fecha_salida},
                  nombre_hotel: {nombre_hotel}
                  empleados= {"empleados"}
                  """)
            
            
            # Hoteles, reservas, habitaciones, empleados
            insertarTable_wreservas(id_reserva, fecha_llegada, fecha_salida, nombre_hotel, empleados)
           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
















      



# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wHoteles():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_reservas (
                id_hotel SERIAL PRIMARY KEY,
                empleados VARCHAR (100),
                categoria_habitacion (100),
                menu_price_habitacion DECIMAL(10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  



###
createTable_wReservas()
dataframe_wreservas()
#    createTable_wRestaurantes()
#    createTable_wReservas()
#    createTable_wHoteles()
###



'''
5.2.1 Análisis de las preferencias de los clientes
¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
5.2.2 Análisis del rendimiento del restaurante:
¿Qué restaurante tiene el menu_price medio de menú más alto?
¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
5.2.3 Patrones de reserva
¿Cuál es la duración media de la estancia de los clientes de un hotel?
¿Existen periodos de máxima ocupación en función de las fechas de reserva?
5.2.4 Gestión de empleados
¿Cuántos empleados tiene de media cada hotel?
5.2.5 Ocupación e ingresos del hotel
¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de
habitación?
¿Podemos estimar los ingresos generados por cada hotel basándonos en los
menu_prices de las habitaciones y los índices de ocupación?
5.2.6 Análisis de menús
¿Qué platos son los más y los menos populares entre los restaurantes?
23/24 - IABD - Big Data Aplicado
¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los
platos?
5.2.7 Comportamiento de los clientes
¿Existen pautas en las preferencias de los clientes en función de la época del año?
¿Los clientes con preferencias dietéticas específicas tienden a reservar en
restaurantes concretos?
5.2.8 Garantía de calidad
¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas
reales realizadas?
5.2.9 Análisis de mercado
¿Cómo se comparan los menu_prices de las habitaciones de los distintos hoteles y
existen valores atípicos?'''

