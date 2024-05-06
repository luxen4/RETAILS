import psycopg2
from psycopg2 import Error

def insert_data(data):
    connection=None
    try:
        # Esta de Rafa, está desordenada y no traga 
        # connection = psycopg2.connect(user="postgress",password="password", host="localhost", port="5432", database="retail_db")
        
        connection = psycopg2.connect(host='localhost' , port='5432',database='retail_db' , user= 'postgres'    , password='casa1234' )
        cursor = connection.cursor()
        
        create_table_query = '''CREATE TABLE IF NOT EXISTS Employees (
                                nombre VARCHAR(255),
                                apellidos VARCHAR(255),
                                edad VARCHAR(255)
                            );'''

        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully")

        insert_query = """ INSERT INTO Employees (nombre, apellidos, edad) VALUES (%s, %s, %s)"""
        cursor.execute(insert_query, data)

        connection.commit()
        print("Data inserted successfully")

    except (Exception, Error) as error:
        print("Error while inserting data into PostgreSQL:", error)

    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

data_to_insert = ('Adri', 'Laya', 'García')
insert_data(data_to_insert)
