import random
from faker import Faker

def storeName():
    fake = Faker()
    return fake.company()
    #return [fake.company() for _ in range(1)]

def location():
    locations = ["Logroño", "Vitoria", "Pamplona", "Bilbao", "San Sebastián", "Santander"]
    return random.choice(locations)

def mediaEdad():
    return random.randint(18, 100)

def sexo(length=1):
    letters = 'HM'
    return ''.join(random.choice(letters) for _ in range(length))

def ingresos():
    return round(random.uniform(0.0, 6000.0),2)


#store_ID,store_name,location,demographics

shops=[]
for i in range(1, 10):
    shop= [ str(i),storeName(), location(), "Media edad:" +str(mediaEdad()) + "," "Sexo:"+sexo() + "," + "Nivel Ingresos:" +str(ingresos())]
    shops.append(shop)

print(shops)

# crear el csv
# Function to create a new CSV file with sample data
import csv
def create_csv_file(filename, data, headers):
    
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        # Escribir las cabeceras
        writer.writerow(headers)
        # Escribir los datos
        writer.writerows(data)
    print(f"Created {filename}")
    
filename="Spark/data_bda/csv/data_stores.csv"
headers = ["store_id","store_name", "location", "demografics"]
"Media edad: 41, Sexo: H, Nivel Ingresos: 15916"

create_csv_file(filename, shops, headers)
