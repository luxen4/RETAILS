import mysql.connector
import random
from datetime import datetime, timedelta

def dropTable():
    try:
        conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")
        cursor = conexion.cursor()

        sql = """ DROP TABLE IF EXISTS employees; """

        cursor.execute(sql)
        conexion.commit()
        cursor.close()
        conexion.close()

        print("Table 'employees' eliminada exitosamente.")
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido eliminar la tabla.")


def createTable():
    try:
        conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")
        cursor = conexion.cursor()

        sql = """ CREATE TABLE employees(
                employees_id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                apellido VARCHAR(100),
                cargo VARCHAR(100),
                salario NUMERIC(10, 2),
                fecha_contratacion DATE
            );
        """

        cursor.execute(sql)
        conexion.commit()
        cursor.close()
        conexion.close()

        print("Table 'ZAPATILLAS' creada exitosamente.")
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido crear la tabla.")    
        
        
def insertTable(nombre, apellido, cargo, salario, fecha_contratacion):
    try:
        conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")
        cursor = conexion.cursor()

        cursor.execute(""" INSERT INTO employees (nombre, apellido, cargo, salario, fecha_contratacion) VALUES (%s, %s, %s, %s, %s) """, 
                                (nombre, apellido, cargo, salario, fecha_contratacion))

        conexion.commit()
        cursor.close()
        conexion.close()

        print("Registro insertado.")
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido insertar en la tabla.")      
        
        

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


        
dropTable()
createTable()

for _ in range(50):
    nombre = generar_nombre()
    apellido = generar_apellido()
    cargo = generar_cargo()
    salario = generar_sueldo()
    fecha_contratacion = generate_random_date(start_date, end_date)
    
    insertTable(nombre, apellido, cargo, salario, fecha_contratacion)    



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
        
        
