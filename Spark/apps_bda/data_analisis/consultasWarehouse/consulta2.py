import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        
def select2(connection, fecha):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT SUM(revenue) AS total_revenue FROM ventas where date = '" + fecha + "';")
        rows = cursor.fetchall()

        for row in rows: 
            valor_decimal = row[0]  
            valor_numerico = float(valor_decimal)  
            
            linea="Fecha " + fecha + ", Revenue: " + str(valor_numerico) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" 2. ● ¿Cuáles son los ingresos totales generados en una fecha concreta?")

fecha = input("inserte fecha (2024-07-28)")
print(select2(connection, fecha))
