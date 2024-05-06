import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT location, revenue AS revenue_total FROM geografico 
                                GROUP BY location, revenue_total 
                                ORDER BY revenue_total DESC LIMIT 1 """)
        rows = cursor.fetchall()

        for row in rows: 
            location, revenue_total = row
            linea="Localización: " + location + ", total: " + str(revenue_total)
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" 4. ● ¿Cuáles son las regiones con mejores resultados en función de los ingresos?")
# b2 ● ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?
# Meter más de San Sebastián y menos de Logroño,

print("Ajustar para que salgan más locations")
print(select(connection))