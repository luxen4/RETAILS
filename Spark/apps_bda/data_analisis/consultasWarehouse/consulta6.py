# c. Análisis demográfico: store_ID, store_name, demographics, revenue
# ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?


import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT demographics, revenue AS total FROM demografico
                        ORDER BY total asc
                        LIMIT 10 
                       """)
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
        

print(" 4. ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?")
print(select(connection))
    
    
"""
SELECT demographics, SUM(revenue) AS total FROM demografico
GROUP BY demographics
ORDER BY total asc
LIMIT 10 
"""