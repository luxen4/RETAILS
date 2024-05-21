from neo4j import GraphDatabase
import json

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        for item in data:
            # Eliminar la línea que convierte la fecha a una cadena ISO
            # item['Fecha'] = item['Fecha'].isoformat()
            json.dump(item, file)
            file.write('\n')
    print(f"File '{filename}' created successfully!")


class Neo4jClient:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def create_provider(self, session, provider_id, name, location):
        result = session.run("CREATE (p:Provider {id: $id, name: $name, location: $location}) RETURN p", id=provider_id, name=name, location=location)
        return result.single()[0]

if __name__ == "__main__":
    # URI, usuario y contraseña de la base de datos Neo4j
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "your_password"

    # Crear instancia de Neo4jClient
    neo4j_client = Neo4jClient(uri, user, password)
    neo4j_client.connect()

    # Lista de proveedores a insertar
    proveedores = [
        {"ID": 1, "name": "Proveedor A", "location": "Ciudad X"},
        {"ID": 2, "name": "Proveedor B", "location": "-"},
        {"ID": 3, "name": "Proveedor C", "location": "Ciudad Z"},
        {"ID": 4, "name": "Proveedor D", "location": "Ciudad W"}
    ]

    with neo4j_client._driver.session() as session:
        for proveedor in proveedores:
            provider = neo4j_client.create_provider(session, proveedor["ID"], proveedor["name"], proveedor["location"])
            print(f"Proveedor creado: {provider['name']} ({provider['location']}) con ID: {proveedor['ID']}")


    create_json_file('./../../1_data_bda/json/dataProveedores.json', proveedores)
    print("generado")

    neo4j_client.close()