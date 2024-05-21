import psycopg2
import csv


def dropTableStores():
    try:
        connection = psycopg2.connect(host='localhost', port='9999', database='bdaretails', user= 'user1', password='retails' )
        cursor = connection.cursor()
        cursor.execute(""" DROP TABLE IF EXISTS stores; """)
        connection.commit()
        print("Tabla Stores eliminada correctamente.")
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error:", e)


def createTableStores():
    try:
        connection = psycopg2.connect(host='localhost', port='9999', database='bdaretails', user= 'user1', password='retails' )
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id SERIAL PRIMARY KEY,
                store_name VARCHAR(100),
                location VARCHAR(100),
                demographics VARCHAR(100)
            );
        """)
        connection.commit()
        print("Tabla Stores creada correctamente.")
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error:", e)


     
def insertar_Stores(store_id, store_name, location, demographics):
    try:
        connection = psycopg2.connect(host='localhost', port='9999', database='bdaretails', user= 'user1', password='retails' )
        
        cursor = connection.cursor()
        cursor.execute("INSERT INTO stores (store_id, store_name, location, demographics) VALUES (%s, %s, %s, %s);", 
                    (store_id, store_name, location, demographics))

        print("Datos Insertados")
        connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
        cursor.close()
        connection.close()

        print("Datos cargados correctamente en tabla STORES.")
        
    except psycopg2.Error as e:
        print("Error al insertar datos:", e)


# Leer el csv e insertar
def read_csv_file(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            store_id, store_name, location, demographics= row
            insertar_Stores(store_id, store_name, location, demographics)


if __name__ == "__main__":
    
    dropTableStores()
    createTableStores()          
            
    filename='Spark/data_Retails/csv/data_stores.csv'
    read_csv_file(filename)
    
    
    
    
# ---> Lee el archivo csv e inserta en la base de datos de Postgres