import psycopg2, random

store_name=""
location=""
nivelingresos=0

def localizacion():
    probabilidad = random.random()
    if probabilidad < 0.20:     location = "Logroño"
    elif probabilidad < 0.40:   location = "Pamplona"
    elif probabilidad < 0.60:   location = "Vitoria"
    elif probabilidad <= 0.80:  location = "Santander"
    else:                       location = None
    
    return location

def demographics_datos():
    mediaedad = random.randint(18, 75)
    sexo = ''.join(random.choices('MH', k=1))
    nivelingresos = random.randint(10000, 50000)
    
    probabilidad = random.random()
    if probabilidad < 0.90:   
        demographics= "Media edad: " + str(mediaedad) + ", Sexo: " + sexo + ", Nivel Ingresos: " + str(nivelingresos)
    else:
        demographics = None
    
    return demographics


def insertarStores(store_name, location, demographics):
    try:
        #connection = psycopg2.connect(host='my_postgres_service' , port='5432',database='retail_db' , user= 'postgres'    , password='casa1234' )
        connection = psycopg2.connect(host='localhost' , port='5432',database='retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()  
        
        cursor.execute("INSERT INTO Stores (store_name, location, demographics) VALUES (%s, %s, %s);", (store_name, location, demographics))
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Stores.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        


def principal():
    for x in range(1, 8):
        probabilidad = random.random()
        if probabilidad < 0.20:
            store_name = None
        else:
            store_name = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))

        location = localizacion()
        demographics = demographics_datos() 

        insertarStores(store_name, location, demographics)


if __name__ == "__main__":
    principal()