# d. Análisis temporal:   date,  ,revenue
# ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente)?

import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        
def selectYears(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT EXTRACT(YEAR FROM date) AS year, SUM(revenue) AS total_revenue
                            FROM temporal
                            GROUP BY EXTRACT(YEAR FROM date)
                            ORDER BY year ASC
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            year = row[0]  
            revenue = row[1]
            
            linea="Year " + str(year) + ", SumaRevenue: " + str(revenue) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        
        
def selectMonths(connection):
    info2='\n'
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("""SELECT EXTRACT(MONTH FROM date) AS month, SUM(revenue) AS total_revenue
                            FROM temporal
                            GROUP BY EXTRACT(MONTH FROM date)
                            ORDER BY month ASC
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            month = row[0]  
            revenue = row[1]
            
            linea="Month " + str(month) + ", SumaRevenue: " + str(revenue) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        
def selectFecha(connection):
    info2='\n'
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("""SELECT DATE(date) AS day, SUM(revenue) AS total_revenue
                            FROM temporal
                            GROUP BY DATE(date)
                            ORDER BY day ASC
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            month = row[0]  
            revenue = row[1]
            
            linea="Dia " + str(month) + ", SumaRevenue: " + str(revenue) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        
        
    
def selectWeek(connection):
    info2='\n'
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("""SELECT TO_CHAR(DATE_TRUNC('week', date), 'YYYY-MM-DD') AS start_of_week, SUM(revenue) AS total_revenue
                                FROM temporal
                                GROUP BY DATE_TRUNC('week', date)
                                ORDER BY start_of_week ASC
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            month = row[0]  
            revenue = row[1]
            
            linea="Semana " + str(month) + ", SumaRevenue: " + str(revenue) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        


def selectDayOFWeek(connection):
    info2='\n'
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("""SELECT TO_CHAR(date, 'Dy') AS day_of_week, 
                            SUM(revenue) AS total_revenue
                                FROM temporal
                                GROUP BY TO_CHAR(date, 'Dy')
                                ORDER BY CASE 
                                    WHEN TO_CHAR(date, 'Dy') = 'Mon' THEN 1
                                    WHEN TO_CHAR(date, 'Dy') = 'Tue' THEN 2
                                    WHEN TO_CHAR(date, 'Dy') = 'Wed' THEN 3
                                    WHEN TO_CHAR(date, 'Dy') = 'Thu' THEN 4
                                    WHEN TO_CHAR(date, 'Dy') = 'Fri' THEN 5
                                    WHEN TO_CHAR(date, 'Dy') = 'Sat' THEN 6
                                    WHEN TO_CHAR(date, 'Dy') = 'Sun' THEN 7
END ASC
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            month = row[0]  
            revenue = row[1]
            
            linea="Day of week " + str(month) + ", SumaRevenue: " + str(revenue) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)



print(" . ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente?")
print(selectYears(connection))

print(selectMonths(connection))

print(selectFecha(connection))

print(selectWeek(connection))

print(selectDayOFWeek(connection))


"""SELECT EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month, SUM(revenue) AS total_revenue
                            FROM temporal
                            GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
                            ORDER BY year ASC, month ASC
                       """