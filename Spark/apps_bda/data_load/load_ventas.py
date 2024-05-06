import psycopg2
from pyspark.sql import SparkSession

# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_ventas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS ventas (
                ventas_id SERIAL PRIMARY KEY,
                store_id INTEGER,
                store_name VARCHAR (100),
                location VARCHAR (100),
                date DATE,
                product_ID INTEGER,
                quantity_sold DECIMAL (10,2),
                revenue DECIMAL(10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'VENTAS' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)
    
 
# Leer el CSV y cargar los datos en la tabla de PostgreSQL
def insertarTable_ventas():
    
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="localhost", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO ventas ( store_ID, store_name, location, date, product_ID, quantity_sold, revenue) 
                   VALUES ('1','Tienda Laya', 'Alberite', '2024/7/28', 1, 50, 2500 )""")

    """
    for row in csv_reader:
        if any(row):
            cursor.execute("INSERT INTO sales (date, store_ID, store_name, product_ID, product_name, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s)", row)
    """
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla ventas.")
     
    
def dataFrameVentas():
    
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
# a. Análisis de ventas:  store_ID, store_name, ubicación, revenue
# ● ¿Qué tienda tiene los mayores ingresos totales?
# ● ¿Cuáles son los ingresos totales generados en una fecha concreta?
# ● ¿Qué producto tiene la mayor cantidad vendida?
        
      
        bucket_name = 'my-local-bucket'
        file_name = 'sales_data.csv' 
        df_sales = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_sales.show()
        
        bucket_name = 'my-local-bucket'
        file_name = 'data_stores.csv' 
        df_stores = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_stores.show()
        
        
        
        df = df_sales.join(df_stores.select("store_id","store_name","location", "demographics"), "store_id", "left")
        #df = df.withColumnRenamed("nombre", "restaurante_name")   # Cambiar el nombre de la columna
        df.show()
        

        
       
        
        
        '''
          # Stores, reservas, habitaciones, empleados
        bucket_name = 'my-local-bucket' 
        file_name = 'data_hoteles.json'
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        df_hoteles.show()
        
        
        
        
        
        
        
        
        bucket_name = 'my-local-bucket' 
        file_name='data_menus'      
        df_menus = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_menus.show()
        
        
        df = df.join(df_menus.select("id_restaurante","id_menu","precio"), "id_restaurante", "left") #### METER UN NOMBRE DE MENU
        df.show()
        
        
        bucket_name = 'my-local-bucket'
        file_name = 'platos'
        df_platos= spark.read.json(f"s3a://{bucket_name}/{file_name}") 
        #df_restaurantes.show()
        df = df.join(df_platos.select("id_restaurante","id_hotel"), "id_restaurante", "left")
        '''
        
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "fecha_llegada"]]
        df = df[[col for col in df.columns if col != "fecha_salida"]]
        df = df[[col for col in df.columns if col != "tipo_habitacion"]]
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
            restaurante_name=row["restaurante_name"],
            id_menu=row["id_menu"]
            menu_price=row["precio"]
            
            print(f"""
                  id_reserva-Cliente: {id_reserva}, 
                  restaurante_name: {restaurante_name},
                  id_menu: {id_menu},
                  menu_price: {menu_price}
                  """)
            
            insertarTable_ventas( id_hotel, empleados, categoria_habitacion, price_habitacion)
            
            
            

            
           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

    
    
    
    
    
    
    
    
    
    
    
    
    print()

###
createTable_ventas()
dataFrameVentas()





















# b. Análisis geográfico: store_ID, store_name, ubicación, revenue
# ● ¿Cuáles son las regiones con mejores resultados en función de los ingresos?
# ● ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?


# c. Análisis demográfico: store_ID, store_name, demographics, revenue
# ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?
# ● ¿Existen productos específicos preferidos por determinados grupos demográficos?


# d. Análisis temporal:   date,  ,revenue
# ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente)?
# ● ¿Existen tendencias estacionales en las ventas?






