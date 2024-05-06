import psycopg2
from psycopg2 import sql
import random
from datetime import datetime, timedelta


# Función para generar un nombre aleatorio
def generar_nombre():
    nombres = ["Juan", "María", "Pedro", "Ana", "Luis", "Elena", "Carlos", "Laura", "David", "Sofía"]
    return random.choice(nombres)

# Función para generar una edad aleatoria entre 18 y 80 años
def generar_edad():
    return random.randint(18, 80)

# Función para generar una sueldo aleatoria entre 2000 y 4000 años
def generar_sueldo():
    return random.randint(2000, 4000)

# Función para generar un apellido aleatorio
def generar_apellido():
    apellidos = ["García", "Rodríguez", "Gómez", "Fernández", "López", "Martínez", "Sánchez", "Pérez", "González", "Ramírez"]
    return random.choice(apellidos)

# Función para generar un apellido aleatorio
def generar_cargo():
    apellidos = ["Gerente", "Asistente de ventas", "Cajero", "Vendedor", 
                 "Asistente de almacén", "Jefe de ventas", "Encargado de inventario", "Supervisor de cajas", "Asesor de compras", "Ramírez"]
    
    return random.choice(apellidos)


def generar_dni():
    numbers = '0123456789'
    dni_number = ''.join(random.choice(numbers) for _ in range(8))
    
    letter = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    dni_letter= ''.join(random.choice(letter) for _ in range(1))  
    
    return dni_number + dni_letter


# Definir el rango de fechas
start_date = datetime(2000, 1, 1)
end_date = datetime(2022, 12, 31)
def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

empleados=[]
for _ in range(10):
    empleado=(generar_nombre(), generar_apellido(), generar_cargo(), generar_sueldo(), generate_random_date(start_date, end_date))
    empleados.append(empleado)

try:
    # Conectar a la base de datos PostgreSQL
    conexion = psycopg2.connect(
        host="localhost",
        port="5432",
        database="retail_db",
        user="postgres",
        password="casa1234"
    )

    # Crear un cursor para ejecutar comandos SQL
    cursor = conexion.cursor()

    # Consulta SQL para insertar empleados en la tabla
    sql_insert = sql.SQL("""
    INSERT INTO empleados (nombre, apellido, cargo, salario, fecha_contratacion)
    VALUES (%s, %s, %s, %s, %s)
    """)

    # Iterar sobre los empleados y ejecutar la consulta de inserción para cada uno
    for empleado in empleados:
        cursor.execute(sql_insert, empleado)
    conexion.commit()

    print("Los empleados han sido insertados correctamente en la tabla.")

except psycopg2.Error as e:
    print("Error al insertar empleados:", e)

finally:
    # Cerrar el cursor y la conexión
    if cursor:
        cursor.close()
    if conexion:
        conexion.close()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

'''
Asistente de almacén
Jefe de ventas
Encargado de inventario
Supervisor de cajas
Asesor de compras
Asistente de atención al cliente
Coordinador de logística
Gerente de operaciones
Supervisor de seguridad
Encargado de marketing offline'''