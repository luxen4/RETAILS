from neo4j import GraphDatabase

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

    def get_all_providers(self, session):
        result = session.run("MATCH (p:Provider) RETURN p")
        return result

if __name__ == "__main__":
    # URI, usuario y contraseña de la base de datos Neo4j
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "your_password"
    
    


    # Crear instancia de Neo4jClient
    neo4j_client = Neo4jClient(uri, user, password)
    neo4j_client.connect()

    # Crear una sesión de Neo4j
    with neo4j_client._driver.session() as session:
        # Consultar todos los proveedores
        proveedores = neo4j_client.get_all_providers(session)

        # Imprimir los proveedores
        print("Proveedores en la base de datos:")
        for record in proveedores:
            proveedor = record['p']
            print(f"ID: {proveedor['id']}, Nombre: {proveedor['name']}, Ubicación: {proveedor['location']}")

    # Cerrar la conexión con Neo4j
    neo4j_client.close()