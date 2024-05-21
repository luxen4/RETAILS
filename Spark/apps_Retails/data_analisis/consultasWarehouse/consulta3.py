import psycopg2
connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        
def select(connection):
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT product_ID, SUM(quantity_sold) AS total FROM ventas
                        GROUP BY product_ID
                        ORDER BY total desc, product_ID desc
                        LIMIT 5
                       """)
        # Se vende mucho cada dia, bajar el random
        
        rows = cursor.fetchall()

        for row in rows: 
            print(row)
            product_id, total = row
            linea="Product ID: " + str(product_id) + ", Total: " + str(total) + " €"
            info2 = info2 + linea + '\n'

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)
        

print(" 3. ● ¿Qué producto tiene la mayor cantidad vendida?")
print(select(connection))





'''
warehouse_retail_db=# select * from sales;
 ingreso_id |    date    | store_id | product_id | quantity_sold | revenue 
------------+------------+----------+------------+---------------+---------
       2001 | 2024-08-06 |        3 | 9          |         19.00 | 4233.00
       
       
warehouse_retail_db=# select * from products;
 product_id |       product_name        | priceperunit 
------------+---------------------------+--------------
          1 | Camisa cuadros roja/negra |        10.99
          
          
cursor.execute(""" SELECT p.product_ID, p.product_name, SUM(sa.quantity_sold) AS total FROM sales sa
                        INNER JOIN products p ON p.product_ID = sa.product_ID
                        GROUP BY sa.product_ID, s.product_name
                        ORDER BY total DESC
                        LIMIT 2
                        """)'''
                        