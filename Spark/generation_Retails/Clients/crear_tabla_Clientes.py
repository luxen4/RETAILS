import mysql.connector

def dropTabla():
    conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")
    cursor = conexion.cursor()

    sql = """ DROP TABLE IF EXISTS clients """
    cursor.execute(sql)
    conexion.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()

    print("Tabla 'CLIENTS' eliminada exitosamente.")


def crearTabla():

    conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="retail_db")
    cursor = conexion.cursor()

    sql = """
            CREATE TABLE IF NOT EXISTS clients(
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_name VARCHAR(100),
                edad INT,
                apellidos VARCHAR(100),
                dni VARCHAR(20)
            )
            """

    cursor.execute(sql)
    conexion.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()

    print("Table 'CLIENTS' creada exitosamente.")
    
    
dropTabla()
crearTabla()   
    
    
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

consulta = """ INSERT INTO clients (client_name, edad, apellidos, dni) VALUES (%s, %s, %s, %s) """

# Datos del cliente a insertar
for _ in range(10):
    cliente = (generar_nombre(), generar_edad(), generar_apellido(), generar_dni())
    cursor.execute(consulta, cliente)
    conexion.commit()

cursor.close()
conexion.close() 
    
    
    
    
    
    # email VARCHAR(255) NOT NULL,
    # bird_date DATE
    # created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP