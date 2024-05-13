from pymongo import MongoClient

# Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
client = MongoClient()

db = client["products"]
ropa_collection = db["ropa"]            # Accede a la colección "ropa"

# Lista de productos, que vengan desde txt también
productos = [
    { "id": 1, "tipo": "pantalones", "talla": "M", "color": "azul"},
    { "id": 2, "tipo": "camisa", "talla": "L", "color": "blanco"},
    { "id": 3, "tipo": "botas", "talla": "42", "color": "negro"},
    { "id": 4, "tipo": "cinturón", "talla": "única", "color": "marrón"},
    { "id": 5, "tipo": "chaqueta", "talla": "XL", "color": "verde"},
    { "id": 6, "tipo": "vestido", "talla": "S", "color": "rojo"},
    { "id": 7, "tipo": "falda", "talla": "M", "color": "negro"},
    { "id": 8, "tipo": "sombrero", "talla": "única", "color": "azul"},
    { "id": 9, "tipo": "guantes", "talla": "M", "color": "gris"},
    { "id": 10, "tipo": "bufanda", "talla": "única", "color": "blanco"}
]

# Inserta la lista de productos como un único documento en la colección
ropa_collection.insert_one({"productos": productos})

# Imprime todos los documentos en la colección "ropa"
print("Contenido de la colección 'ropa':")
for ropa in ropa_collection.find():
    print(ropa)
    
# Crear un .json