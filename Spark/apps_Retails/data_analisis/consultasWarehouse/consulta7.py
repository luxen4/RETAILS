import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        # ES PRODUCT ID # CORREGIR CUANDO SE PUEDA  ;(
        cu rsor = connection.cursor()
        cursor.execute("""SELECT demographics, revenue FROM demografico 
                       where demographics= 'Media edad: 41, Sexo: H, Nivel Ingresos: 24298'
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            location, total = row
            linea="Demografía: " + str(location) + ", Revenue total: " + str(total)
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" 4. ● ¿Existen productos específicos preferidos por determinados grupos demográficos?")
print(select(connection))

# ● ¿Existen productos específicos preferidos por determinados grupos demográficos?
