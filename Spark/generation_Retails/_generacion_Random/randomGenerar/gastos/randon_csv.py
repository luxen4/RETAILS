# Supongamos que el formato es tipo_gasto,monto,fecha
import random
import csv, random
import datetime
import string


# Función para generar un nombre aleatorio
def generar_Concepto():
    nombres = ["Luz", "Agua", "Gas", "Limpieza", "Otros"]
    return random.choice(nombres)

def generafecha():
    fecha_inicial = datetime.date(2000, 1, 1)
    fecha_final = datetime.date(2024, 12, 31)

    diferencia_dias = (fecha_final - fecha_inicial).days
    fecha_aleatoria = fecha_inicial + datetime.timedelta(days=random.randint(0, diferencia_dias))
    fecha_aleatoria_str = fecha_aleatoria.strftime("%Y-%m-%d")

    #print("Fecha aleatoria generada:", fecha_aleatoria_str)
    return fecha_aleatoria_str


def generar_id_hotel():
    id_hotel = random.randint(1, 1000)
    return id_hotel


def generar_Monto():
    return random.randint(0, 10000)

def generar_Pagado():
    nombres = ["Si", "No"]
    return random.choice(nombres)


def generate_random_string1(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def generate_random_string(length=3):
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join(random.choice(letters) for _ in range(length))


# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["fecha", "referencia", "hotel_id", "concepto", "monto", "pagado"])  # Escribir el encabezado
        
        for atributo in data:
            cascos = atributo.split('*')
            fecha=cascos[0]
            referencia=cascos[1]
            hotel_id=cascos[2]
            monto=cascos[3]
            concepto=cascos[4]
            pagado=cascos[5]
            writer.writerow([fecha, referencia, hotel_id, monto, concepto, pagado])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")



gastos=[]

def genera():
    for _ in range(1000):
        gasto = "" + str(generafecha()) + "*" + str(generate_random_string(5)) + "*" + str(generar_id_hotel()) + "*" +  str(generar_Concepto()) + "*" + str(generar_Monto()) + "*" + str(generar_Pagado()) + "\n"
        gastos.append(gasto)
    return gastos


# fecha,id_hotel,concepto,monto,pagado
data = genera()
create_csv_file("./csv/gastos.csv", data)


'''
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
    
    return dni_number + dni_letter'''