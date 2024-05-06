import psycopg2

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
     
    
# Crear una tabla para responder a las preguntas de ANALISIS-GEOGRAFICO en WAREHOSE
def createTable_geografico():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS geografico (
                geografico_ID SERIAL PRIMARY KEY,
                store_id INTEGER,
                store_name VARCHAR (100),
                ubicacion VARCHAR (100),
                revenue DECIMAL(10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'GEOGRAFICO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


# Crear una tabla para responder a las preguntas de ANALISIS-DEMOGRAFICO en WAREHOSE
def createTable_demografico():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
                    CREATE TABLE IF NOT EXISTS demografico (
                    demografico_ID SERIAL PRIMARY KEY,
                    store_id INTEGER,
                    store_name VARCHAR (100),
                    demographics VARCHAR (100),
                    revenue DECIMAL(10,2)
                    );
                """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'DEMOGRAFICO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


# Crear una tabla para responder a las preguntas de ANALISIS-TEMPORAL en WAREHOSE
def createTable_temporal():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
                    CREATE TABLE IF NOT EXISTS temporal (
                    temporal_ID SERIAL PRIMARY KEY,
                    date DATE,
                    revenue DECIMAL(10,2)
                );
                """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'TEMPORAL' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)


###
createTable_ventas()
#createTable_geografico()
#createTable_demografico()
#createTable_temporal()
insertarTable_ventas()

















# a. Análisis de ventas:  store_ID, store_name, ubicación, revenue
# ● ¿Qué tienda tiene los mayores ingresos totales?
# ● ¿Cuáles son los ingresos totales generados en una fecha concreta?
# ● ¿Qué producto tiene la mayor cantidad vendida?


# b. Análisis geográfico: store_ID, store_name, ubicación, revenue
# ● ¿Cuáles son las regiones con mejores resultados en función de los ingresos?
# ● ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?


# c. Análisis demográfico: store_ID, store_name, demographics, revenue
# ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?
# ● ¿Existen productos específicos preferidos por determinados grupos demográficos?


# d. Análisis temporal:   date,  ,revenue
# ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente)?
# ● ¿Existen tendencias estacionales en las ventas?






