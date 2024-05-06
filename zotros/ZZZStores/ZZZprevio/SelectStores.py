import psycopg2
def select_demographics(store_ID):

    try:
        connection = psycopg2.connect(host='localhost', port='5432', database='retail_db', user= 'postgres', password='casa1234' )
        cursor = connection.cursor()
        cursor.execute("SELECT demographics FROM stores where store_ID= '" + str(store_ID) + "' ")
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            print(row)
            demographics = row
            linea="Demographics: " + str(demographics)
            info2 = linea 

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


def select(store_name):
    info2=''
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("SELECT store_ID FROM stores where store_name = '" + store_name + "';")
        rows = cursor.fetchall()

        for row in rows: 
            store_ID = row
            #linea="Store id: " + str(store_ID)
            info2 = info2 + store_ID

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


select_demographics(2)