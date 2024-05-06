from pymongo import MongoClient

# Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
client = MongoClient()

db = client["products"]
ropa_collection = db["ropa"]  # Accede a la colección "ropa"

# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
consulta = { "tipo": "pantalones" }

# Ejecuta la consulta y obtén los resultados
resultados = ropa_collection.find(consulta)
resultados = ropa_collection.find()

# Imprime los resultados
print("Productos encontrados:")
for producto in resultados:
    print(producto)
    
    print("ID del documento:", producto["_id"])
    lista_productos = producto["productos"]
    print("Productos en este documento:")
    
    for p in lista_productos:
        print("ID:", p["id"])
        print("Tipo:", p["tipo"])
        print("Talla:", p["talla"])
        print("Color:", p["color"])
        print()  # Agrega una línea en blanco entre cada producto