import mysql.connector
import random, string

conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="retail_db")
cursor = conexion.cursor()

# Función para generar un nombre aleatorio
def generar_nombre():
    nombres = ["Juan", "María", "Pedro", "Ana", "Luis", "Elena", "Carlos", "Laura", "David", "Sofía"]
    return random.choice(nombres)

# Función para generar una edad aleatoria entre 18 y 80 años
def generar_edad():
    return random.randint(18, 80)

# Función para generar un apellido aleatorio
def generar_apellido():
    apellidos = ["García", "Rodríguez", "Gómez", "Fernández", "López", "Martínez", "Sánchez", "Pérez", "González", "Ramírez"]
    return random.choice(apellidos)

def generar_dni():
    numbers = '0123456789'
    dni_number = ''.join(random.choice(numbers) for _ in range(8))
    
    letter = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    dni_letter= ''.join(random.choice(letter) for _ in range(1))  
    
    return dni_number + dni_letter

consulta = """ INSERT INTO clients (nombre, edad, apellidos, dni) VALUES (%s, %s, %s, %s) """

# Datos del cliente a insertar
for _ in range(10):
    cliente = (generar_nombre(), generar_edad(), generar_apellido() + " " + generar_apellido(), generar_dni())
    cursor.execute(consulta, cliente)
    conexion.commit()

cursor.close()
conexion.close()