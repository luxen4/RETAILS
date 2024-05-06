import psycopg2
import csv, json
conexion = psycopg2.connect(host="localhost", port="5432", database="retail_db", user="postgres", password="casa1234")
cursor = conexion.cursor()


def generaJSON(nombre_archivo, resultados):

    # Convertir los resultados a una lista de diccionarios
    stores_list = []
    for cliente in resultados:
        cliente_dict = {
            "store_id": cliente[0],
            "store_nombre": cliente[1],
            "location": cliente[2],
            "demographics": cliente[3]
        }
        stores_list.append(cliente_dict)

    # Guardar los datos en el archivo JSON
    with open(nombre_archivo, "w") as archivo_json:
        json.dump(stores_list, archivo_json, indent=4)

    print(f"Los datos se han guardado en el archivo {nombre_archivo}")


def generaCSV(nombre_archivo, resultados):
    with open(nombre_archivo, "w", newline="") as file:
        writer = csv.writer(file)
        
        writer.writerow(['store_ID','store_name','location','demographics'])# Escribir el encabezado
        for store in resultados:
            writer.writerow(store)

    print("Archivo CSV generado exitosamente.")


try:
    # Establecer la conexión a la base de datos PostgreSQL
  
    sql_query = "SELECT * FROM stores"
    cursor.execute(sql_query)
    resultados = cursor.fetchall()

except psycopg2.Error as e:
    print("Error al ejecutar la consulta SQL:", e)

finally:
    # Cerrar el cursor y la conexión
    if cursor:
        cursor.close()
    if conexion:
        conexion.close()
        
        
        
 # Guardar los resultados en un archivo CSV
nombre_archivo = "./../../1_data_bda/csv/data_stores.csv"
generaCSV(nombre_archivo, resultados)      
       
        
# Nombre del archivo CSV
nombre_archivo = "./../../1_data_bda/json/data_stores.json"
generaJSON(nombre_archivo, resultados)
