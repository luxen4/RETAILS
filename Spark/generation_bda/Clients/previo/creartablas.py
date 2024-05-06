import mysql.connector

conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="retail_db")
cursor = conexion.cursor()

sql = """
CREATE TABLE IF NOT EXISTS clients(
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_name VARCHAR(100),
    edad INT,
    apellidos VARCHAR(100),
    dni VARCHAR(20)
)
"""
# email VARCHAR(255) NOT NULL,
# bird_date DATE
# created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
cursor.execute(sql)
conexion.commit()

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

print("Table 'CLIENTS' creada exitosamente.")