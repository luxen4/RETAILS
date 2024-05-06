from time import sleep                      # Ejecutar con el botón derecho
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import random  # Importa la librería random
                    
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    
    #store_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
    #product_id = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
    
    store_id = random.randint(1, 21)
    product_id = random.randint(1, 10)
    quantity_sold = random.randint(1, 100)
    revenue = round(random.uniform(100.0, 10000.0), 2)
    
    
    message = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "store_id": store_id,
        "product_id": product_id,
        "quantity_sold": quantity_sold,
        "revenue": revenue
    }
    
    
    producer.send('sales_stream', value=message)
    sleep(1)  # Adjust frequency as needed
    

# SE EJECUTA ASÍ
# PS C:\Users\Adrian\Downloads\BDA_Adrian> & C:/Users/Adrian/AppData/Local/Microsoft/WindowsApps/python3.10.exe 
# "c:/Users/Adrian/Downloads/BDA_Adrian/Tema 4/Spark/apps/AdriKafka/kafka_consumer.py"


'''
  # Obtener la hora actual en la zona horaria local
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).astimezone()
    
    message = {
        "timestamp": int(now.timestamp() * 1000),
        "store_id": store_id,
        "product_id": product_id,
        "quantity_sold": quantity_sold,
        "revenue": revenue
    }
    '''