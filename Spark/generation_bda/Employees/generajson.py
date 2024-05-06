import psycopg2
import json

# Establecer la conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")
cursor = conexion.cursor()

sql_query = "SELECT * FROM emlpeados"

try:
    cursor.execute(sql_query)
    resultados = cursor.fetchall()

    # Convertir los resultados en una lista de diccionarios
    empleados = []
    for empleado in resultados:
        empleado_dict = {
            "id": empleado[0],
            "nombre": empleado[1],
            "apellido": empleado[2],
            "cargo": empleado[3],
            "salario": float(empleado[4]),
            "fecha_contratacion": str(empleado[5])
        }
        empleados.append(empleado_dict)

    # Guardar la lista de empleados en un archivo JSON
    nombre_archivo = "./../../1_data_bda/json/data_employees.json"
    with open(nombre_archivo, "w") as file:
        json.dump(empleados, file, indent=4)

    print("Archivo JSON generado exitosamente.")

except psycopg2.Error as e:
    print("Error al ejecutar la consulta SQL:", e)

finally:
    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()