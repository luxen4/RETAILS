from kafka import KafkaConsumer                     # type: ignore  El interprete debe ser el 10 crtl+shift+p
import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timezone

# Este consumer es exerno a S3

def insertar_sales_stream(fecha, store_id, product_id, quantity_sold, revenue):
    try:
        connection = psycopg2.connect(host='localhost', port='5432',database='retail_db', user= 'postgres', password='casa1234' )
        # print(revenue)
        
        cursor = connection.cursor() 
        cursor.execute("INSERT INTO sales_stream (fecha, store_id, product_id, quantity_sold, revenue) VALUES (%s, %s, %s, %s, %s);", 
                    (fecha, store_id, product_id, quantity_sold, revenue))
        


        connection.commit()
        cursor.close()
        connection.close()
        print("Registro insertado correctamente en sales_stream.")
        
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


for message in consumer:
    #print(f"Received message: {message.value}")
    try:
        json_str = message.value                    # Cadena JSON
        json_obj = json.loads(json_str)             # Convertir la cadena JSON a un objeto JSON
        
        #_____
        now = datetime.now(timezone.utc).timestamp()  
        timestamp = json_obj["timestamp"]                       # timestamp del mensaje               
        diferencia_tiempo = now - (timestamp / 1000)
        #_____
                                                                    
        if diferencia_tiempo <= 1:                              # Tiempo es menor a 1 segundo, procesar el mensaje
            print("Mensaje válido")
            print(f"Received message: {message.value}")
            
            fecha = datetime.utcfromtimestamp(timestamp / 1000)     # Dividir por 1000 para convertir de milisegundos a segundos
            #fecha_formateada = fecha.strftime('%m-%d-%Y')           # Formatear la fecha para eliminar los decimales de los segundos
            fecha_formateada = fecha.strftime('%Y-%m-%d')
            
            if "product_id" in json_obj:
                product_id = json_obj["product_id"]
            elif "product_ids" in json_obj:
                product_id = json_obj["product_ids"]
            else:
                raise KeyError("El mensaje no contiene un campo 'product_id' o 'product_ids'")        
                      
            store_id = json_obj["store_id"]
            quantity_sold = json_obj["quantity_sold"]
            revenue = json_obj["revenue"]
            
            print(fecha_formateada)
            
            insertar_sales_stream(fecha_formateada, store_id, product_id, quantity_sold, revenue)
            
        else:
            print()
            print("Mensaje antiguo, ignorado:", message)

    except json.JSONDecodeError as e:
        print()
        print("Error al decodificar el JSON:", e)
consumer.close()







'''
 # Obtener la hora actual en la zona horaria local
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).astimezone()
    
    #store_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
    #product_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
    
'''




















