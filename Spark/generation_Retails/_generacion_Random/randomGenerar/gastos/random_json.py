

import json, random, datetime, string

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)
        file.write('\n')
    print(f"File '{filename}' created successfully!")
    
    
    
    

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


def create_text_file(filename, content):
    with open(filename, 'w') as file:
        file.write(content)
    print(f"File '{filename}' created successfully!")


gastos=[]
def genera():
    for _ in range(1000):
        gasto={"fecha":str(generafecha()), "referencia":str(generate_random_string(5)), 
               "hotel_id":str(generar_id_hotel()),"concepto":str(generar_id_hotel()), "monto":str(generar_Monto()), "pagado":str(generar_Pagado())}
        gastos.append(gasto)
    return gastos

data = genera()
file_name='./json/gastos.json'
create_json_file(file_name, data)

  