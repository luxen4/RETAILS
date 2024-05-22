import mysql.connector
import csv, json

conexion = mysql.connector.connect( host="localhost", user="root", password="alberite", database="retail_db")
# Crear un cursor
cursor = conexion.cursor()

# Consulta SQL para seleccionar todos los clientes
sql = "SELECT * FROM clients"

# Ejecutar la consulta
cursor.execute(sql)

# Obtener todos los resultados
resultados = cursor.fetchall()

# Nombre del archivo JSON
# Nombre del archivo CSV
nombre_archivo = "Spark/data_Retails/json/data_clientsAAAAAAAAAA.json"

# Convertir los resultados a una lista de diccionarios
clientes_list = []
for cliente in resultados:
    cliente_dict = {
        "id": cliente[0],
        "nombre": cliente[1],
        "edad": cliente[2],
        "apellidos": cliente[3],
        "dni": cliente[4]
    }
    clientes_list.append(cliente_dict)

# Guardar los datos en el archivo JSON
with open(nombre_archivo, "w") as archivo_json:
    json.dump(clientes_list, archivo_json, indent=4)

print(f"Los datos se han guardado en el archivo {nombre_archivo}")

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()