import csv
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
nombre_archivo = "Spark/data_Retails/csv/data_employeesAAA.csv"

with open(nombre_archivo, "w", newline="") as file:
    writer = csv.writer(file)
    
    writer.writerow(["ID", "Nombre", "Apellido", "Cargo", "Salario", "Fecha Contratación"])# Escribir el encabezado
    for empleado in resultados:
        writer.writerow(empleado)


    print("Archivo CSV generado exitosamente.")
