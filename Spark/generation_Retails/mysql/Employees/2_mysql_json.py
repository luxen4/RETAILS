import psycopg2
import json
import mysql.connector


def selectTable():
    try:
        conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")
        cursor = conexion.cursor()

        sql = """Select * from empleados; """

        cursor.execute(sql)
         # Obtener los resultados
        resultados = cursor.fetchall()
        
        print("Datos OK.")
        return resultados
        
    except Exception:
        #print(f"File '{filename}' not found.")
        print("No se ha podido crear la tabla.")


resultados = selectTable()

empleados=[]
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
nombre_archivo = "Spark/data_Retails/json/data_employeesAAA.json"
with open(nombre_archivo, "w") as file:
    json.dump(empleados, file, indent=4)

print("Archivo JSON generado exitosamente.")
