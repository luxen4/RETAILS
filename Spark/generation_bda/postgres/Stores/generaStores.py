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



'''
import csv, os, random
        
        store_name=""
        location=""
        nivelingresos=0
        
        for x in range(1, 8):
        
            probabilidad = random.random()
            if probabilidad < 0.20:
                store_name = None
            else:
                store_name = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
            
            probabilidad = random.random()
            if probabilidad < 0.20:     location = "Logroño"
            elif probabilidad < 0.40:   location = "Pamplona"
            elif probabilidad < 0.60:   location = "Vitoria"
            elif probabilidad <= 0.80:  location = "Santander"
            else:                       location = None
            
            mediaedad = random.randint(18, 75)
            sexo = store_id = ''.join(random.choices('MH', k=1))
            nivelingresos = random.randint(10000, 50000)
            
            probabilidad = random.random()
            if probabilidad < 0.80:   
                demographics= "Media edad: " + str(mediaedad) + ", Sexo: " + sexo + ", Nivel Ingresos: " + str(nivelingresos)
            else:
                demographics = None'''