from kafka import KafkaConsumer # type: ignore  El interprete debe ser el 10 crtl+shift+p
import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timezone


def select_store_name(store_ID):
    connection = psycopg2.connect(host='localhost', port='5432',database='retail_db', user='postgres', password='casa1234')
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT store_name FROM stores where store_ID = '"+str(store_ID)+"';")
        rows = cursor.fetchall()

        for row in rows: 
            print("La row: " + str(row[0]))
            

            cursor.close()
            connection.close()
            return row[0]

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)

def select_product_name(product_ID):
    connection = psycopg2.connect(host='localhost', port='5432',database='warehouse_retail_db', user='postgres', password='casa1234')
    info2='\n'
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT store_name FROM products where product_ID = '"+product_ID+"';")
        rows = cursor.fetchall()

        for row in rows: 
            print(row)

        cursor.close()
        connection.close()
        return info2

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)



# Set up Kafka consumer
consumer = KafkaConsumer(
    'sales_stream',                       # Topic to subscribe to
    bootstrap_servers=['localhost:9092'], # Kafka broker(s)
    auto_offset_reset='earliest',        # Start from earliest message
    enable_auto_commit=True,             # Commit offsets automatically
    value_deserializer=lambda x: x.decode('utf-8') 
)

def insertarSalesKafka(fecha_formateada, store_ID, product_ID, quantity_sold, revenue):
    try:
        connection = psycopg2.connect(host='localhost' , port='5432',database='retail_db' , user= 'postgres', password='casa1234' )
        # print(revenue)
        
        cursor = connection.cursor() 
        cursor.execute("INSERT INTO sales (date, store_ID, product_ID, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s);", 
                    (fecha_formateada, store_ID, product_ID, quantity_sold, revenue))

        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en Sales.")
        
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


for message in consumer:
    #print(f"Received message: {message.value}")
    try:
        json_str = message.value                    # Cadena JSON
        json_obj = json.loads(json_str)             # Convertir la cadena JSON a un objeto JSON
        
        ######################
        now = datetime.now(timezone.utc).timestamp()  
        timestamp = json_obj["timestamp"]                       # timestamp del mensaje               
        diferencia_tiempo = now - (timestamp / 1000)
                                                                    
        if diferencia_tiempo <= 1:                              # Tiempo es menor a 1 segundo, procesar el mensaje
            print("Mensaje válido")
            print(f"Received message: {message.value}")
            
            fecha = datetime.utcfromtimestamp(timestamp / 1000)     # Dividir por 1000 para convertir de milisegundos a segundos
            fecha_formateada = fecha.strftime('%m-%d-%Y')           # Formatear la fecha para eliminar los decimales de los segundos
            
            store_ID = json_obj["store_id"]
            store_name = select_store_name(store_ID)
            
            
            if "product_id" in json_obj:
                product_ID = json_obj["product_id"]
            elif "product_ids" in json_obj:
                product_ID = json_obj["product_ids"]
            else:
                raise KeyError("El mensaje no contiene un campo 'product_id' o 'product_ids'")
            #product_name = select_product_name(product_ID)
            
            quantity_sold = json_obj["quantity_sold"]
            revenue = json_obj["revenue"]
            
            insertarSalesKafka(fecha_formateada, store_ID, product_ID, quantity_sold, revenue)
            
            
            
            
            
        else:
            print()
            #print("Mensaje antiguo, ignorado:", message)

    except json.JSONDecodeError as e:
        print()
        #print("Error al decodificar el JSON:", e)
consumer.close()

























