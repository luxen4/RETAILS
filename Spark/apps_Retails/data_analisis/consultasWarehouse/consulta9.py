
import psycopg2

def selectStationOFyear():
    info2='\n'
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("""WITH Seasons (Name, StartMonth, EndMonth) AS (
                                SELECT 'Spring', 3, 5   UNION ALL  
                                SELECT 'Summer', 6, 8   UNION ALL  
                                SELECT 'Autumn', 9, 11  UNION ALL  
                                SELECT 'Winter', 12, 2
                            )
                            SELECT s.Name AS season, SUM(t.revenue) AS total_revenue FROM temporal t
                            JOIN Seasons s ON MONTH(t.date) BETWEEN s.StartMonth AND s.EndMonth
                            GROUP BY s.Name
                            ORDER BY 
                                CASE s.Name
                                    WHEN 'Spring' THEN 1
                                    WHEN 'Summer' THEN 2
                                    WHEN 'Autumn' THEN 3
                                    ELSE 4
                                END;""")
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

print(selectStationOFyear())