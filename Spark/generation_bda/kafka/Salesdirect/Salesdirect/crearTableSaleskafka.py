import psycopg2

connection = psycopg2.connect(host='localhost' , port='5432',database='warehouse_retail_db', user= 'postgres', password='casa1234')
def createTableSales(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ventas (
            ventas_id SERIAL PRIMARY KEY,
            store_id INTEGER,
            store_name VARCHAR(100),
            location VARCHAR(100), 
            date DATE,
            product_ID INTEGER,
            quantity_sold DECIMAL(10,2),
            revenue DECIMAL(10,2)
        );
        """)
        connection.commit()
        print("Tabla sales creada correctamente.")
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error:", e)


def select(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Stores;")
        rows = cursor.fetchall()

        print("Datos en la tabla 'Stores':")
        for row in rows:
            print(row)

        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        
def insertarStores(store_name, location, demographics):
    try:
        connection = psycopg2.connect(host='localhost' , port='5432',database='retail_db' , user= 'postgres'    , password='casa1234' )

        cursor = connection.cursor() 
        cursor.execute("INSERT INTO Stores (store_name, location, demographics) VALUES (%s, %s, %s);", (store_name, location, demographics))
        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Stores.")
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        
        
choice = 1

match choice:
    case 0:
        select(connection)
        print("Select.")
        
    case 1:
        createTableSalesKafka(connection)

    case 2: 
        print("Insertar Stores")
        import csv, os, random
        
        store_name=""
        location=""
        nivelingresos=0
        
        for x in range(1, 8):
        
            probabilidad = random.random()
            if probabilidad < 0.20:
                store_name = None
            else:
                store_name = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
            
            probabilidad = random.random()
            if probabilidad < 0.20:     location = "Logroño"
            elif probabilidad < 0.40:   location = "Pamplona"
            elif probabilidad < 0.60:   location = "Vitoria"
            elif probabilidad <= 0.80:  location = "Santander"
            else:                       location = None
            
            mediaedad = random.randint(18, 75)
            sexo = store_id = ''.join(random.choices('MH', k=1))
            nivelingresos = random.randint(10000, 50000)
            
            probabilidad = random.random()
            if probabilidad < 0.80:   
                demographics= "Media edad: " + str(mediaedad) + ", Sexo: " + sexo + ", Nivel Ingresos: " + str(nivelingresos)
            else:
                demographics = None
        
            insertarStores(store_name, location, demographics)
        
    case _:
        
        print("Invalid choice. Please try again.")
        
# Para ejecutar esto desde PS
# Es la misma que sales, la del csv


