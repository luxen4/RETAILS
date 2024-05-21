import mysql.connector
import csv

conexion = mysql.connector.connect( host="localhost", user="root", password="alberite", database="retail_db")

cursor = conexion.cursor()
sql = "SELECT * FROM clients"

cursor.execute(sql)
resultados = cursor.fetchall()

for cliente in resultados:
    print(cliente)
    
# Nombre del archivo CSV
nombre_archivo = "Spark/data_Retails/csv/data_clientsADRI.csv"

# Escribir los resultados en el archivo CSV
with open(nombre_archivo, "w", newline="") as archivo_csv:
    escritor_csv = csv.writer(archivo_csv)
    escritor_csv.writerow(["client_ID", "client_name", "client_age", "surnames", "dni"])  # Escribir el encabezado
    
    
    escritor_csv.writerow([i[0] for i in cursor.description])
    escritor_csv.writerows(resultados)

print(f"Los datos se han guardado en el archivo {nombre_archivo}")


cursor.close()
conexion.close()