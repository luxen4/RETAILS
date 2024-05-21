import csv, random
import datetime
# Function to create a new CSV file with sample data

def generafecha():
    fecha_aleatoria_str=''
    
    probabilidad = random.random()
    if probabilidad < 0.05:
        fecha_aleatoria_str = None
    else:
        fecha_inicial = datetime.date(2000, 1, 1)
        fecha_final = datetime.date(2024, 12, 31)

        diferencia_dias = (fecha_final - fecha_inicial).days
        fecha_aleatoria = fecha_inicial + datetime.timedelta(days=random.randint(0, diferencia_dias))
        fecha_aleatoria_str = fecha_aleatoria.strftime("%Y-%m-%d")

    #print("Fecha aleatoria generada:", fecha_aleatoria_str)
    return fecha_aleatoria_str



def create_csv_file(filename):
    filas=[]
    for i in range(1, 2001):
       
        date = generafecha()
        store_ID = random.randint(1, 21)
        product_ID = random.randint(1, 10)
        quantity_sold = random.randint(1, 101)
        revenue = random.randint(50, 5001)

        
        # Probabilidad del 10% de que el product_ID sea nulo o vacío
        probabilidad = random.random()
        if probabilidad < 0.05:
            quantity_sold = None
        # Probabilidad del 5% de que el product_ID sea un valor no válido
        elif probabilidad < 0.1:
            quantity_sold = 0
            
            
        # revenue 
        probabilidad = random.random()
        if probabilidad < 0.05:   revenue = None
        elif probabilidad < 0.1:  revenue = 0
        elif probabilidad < 0.15: revenue = "az"

        fila = [date, store_ID, product_ID, quantity_sold, revenue]
        filas.append(fila) 
        
        # Añadir encabezados
        data = [["date", "store_id","product_id","quantity_sold","revenue"]] + filas

        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)

    print(f"Created {filename}")


file_name="./data_bda/csv/sales_data.csv"
# file_name="/opt/spark-data/csv/sales_data2.csv"   desde dentro
create_csv_file(file_name)
