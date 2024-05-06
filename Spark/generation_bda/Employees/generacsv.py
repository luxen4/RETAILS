import psycopg2
import csv

try:
    # Establecer la conexión a la base de datos PostgreSQL
    conexion = psycopg2.connect(host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")
    cursor = conexion.cursor()
    
    sql_query = "SELECT * FROM empleados"
    cursor.execute(sql_query)
    resultados = cursor.fetchall()

    # Guardar los resultados en un archivo CSV
    nombre_archivo = "./../../1_data_bda/csv/data_employees.csv"
    
    with open(nombre_archivo, "w", newline="") as file:
        writer = csv.writer(file)
        
        writer.writerow(["ID", "Nombre", "Apellido", "Cargo", "Salario", "Fecha Contratación"])# Escribir el encabezado
        for empleado in resultados:
            writer.writerow(empleado)

    print("Archivo CSV generado exitosamente.")

except psycopg2.Error as e:
    print("Error al ejecutar la consulta SQL:", e)

finally:
    # Cerrar el cursor y la conexión
    if cursor:
        cursor.close()
    if conexion:
        conexion.close()