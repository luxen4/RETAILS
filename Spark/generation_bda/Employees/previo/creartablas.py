import psycopg2
try:
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    conexion = psycopg2.connect( host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        
    # Crear un cursor para ejecutar comandos SQL
    cursor = conexion.cursor()

    # Consulta SQL para crear la tabla de empleados
    sql_create_table = """
    CREATE TABLE empleados(
        empleados_id SERIAL PRIMARY KEY,
        nombre VARCHAR(100),
        apellido VARCHAR(100),
        cargo VARCHAR(100),
        salario NUMERIC(10, 2),
        fecha_contratacion DATE
    )
    """
    cursor.execute(sql_create_table)
    conexion.commit()
    print("La tabla de empleados de la tienda ha sido creada correctamente.")
except psycopg2.Error as e:
    print("Error al crear la tabla de empleados de la tienda:", e)

finally:
    # Cerrar el cursor y la conexión
    if cursor:
        cursor.close()
    if conexion:
        conexion.close()