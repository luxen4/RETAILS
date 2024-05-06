from time import sleep                      # Ejecutar con el botón derecho
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import random  # Importa la librería random
                    
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
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
    
    print(message)
    producer.send('sales_stream', value=message)
    sleep(3)  # Adjust frequency as needed
