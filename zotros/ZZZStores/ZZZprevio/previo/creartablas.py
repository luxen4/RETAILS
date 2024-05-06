import psycopg2
def createTableStores():
    try:
        connection = psycopg2.connect(host='localhost', port='5432', database='retail_db', user= 'postgres', password='casa1234' )
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE Stores (
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

createTableStores()

# Crea la tabla stores e inserta los datos en bruto