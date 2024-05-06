import psycopg2

def select():
    info2=''
    try:
        connection = psycopg2.connect(host='localhost' , port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
        cursor = connection.cursor()
        cursor.execute("""SELECT store_name, SUM(revenue) AS total_revenue FROM ventas
                            GROUP BY store_name
                            ORDER BY total_revenue asc
                            LIMIT 3
                       ;""")
        rows = cursor.fetchall()

        for row in rows: 
            store_name, revenueTotal = row
            linea="\nStore name: " + store_name + ", Revenue: " + str(revenueTotal)
            info2 = info2 + linea

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)

print("1. ● ¿Qué tienda tiene los mayores ingresos totales?")
print(select())










'''
# while True:
    print("Menu:")
    
    print("\na. Análisis de ventas:")
    print(" 1. ● ¿Qué tienda tiene los mayores ingresos totales?")
   
    print(" 2. ● ¿Cuáles son los ingresos totales generados en una fecha concreta?")
    print(" 3. ● ¿Qué producto tiene la mayor cantidad vendida?")

    print("\nb. Análisis geográfico:")
    print(" 4. ● ¿Cuáles son las regiones con mejores resultados en función de los ingresos?")
    print(" 5. ● ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?")

    print("\nc. Análisis demográfico:")
    print(" 6. ● ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?")
    print(" 7. ● ¿Existen productos específicos preferidos por determinados grupos demográficos?")

    print("\nd. Análisis temporal:")
    print(" 8. ● ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente, semanalmente, mensualmente)?")
    print(" 9. ● ¿Existen tendencias estacionales en las ventas?")

    
    print("\n10. Exit")
    
    choice = input("Enter your choice (1/2/3/4//5/6/7/8/9/10): ")
    print(choice)
        
    if choice == '1':
        print("1. ● ¿Qué tienda tiene los mayores ingresos totales?")
        print(select(connection))
        
    elif choice == '2':
        print(" 2. ● ¿Cuáles son los ingresos totales generados en una fecha concreta?")
        fecha = input("inserte fecha (2019-03-21)")
        print(consulta2.select2(connection,fecha))
        
        
        
       
    elif choice == '3':
        print("a")
    elif choice == '4':
        print("a")
    elif choice == '5':
        print("a")
    elif choice == '6':
        print("a")
    elif choice == '7':
        print("a")
    elif choice == '8':
        print("a")
    elif choice == '9':
        print("a")
    
    elif choice == '10':
        print("Exit")
    
    elif choice == '6':
        print("a")
    else:
        print("Invalid choice. Please select a valid option.")
'''