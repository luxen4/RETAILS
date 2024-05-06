from neo4j import GraphDatabase
import json, csv

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        for item in data:
            print(item)
            # Eliminar la línea que convierte la fecha a una cadena ISO
            # item['Fecha'] = item['Fecha'].isoformat()
            json.dump(item, file)
            file.write('\n')
    print(f"File '{filename}' created successfully!")



# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "name", "location"])  # Escribir el encabezado
        for provider in data:
            writer.writerow([provider['ID'], provider['name'], provider['location']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")




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
        proveedores_list=[]
        for record in proveedores:
            proveedor = record['p']
            print(f"ID: {proveedor['id']}, Nombre: {proveedor['name']}, Ubicación: {proveedor['location']}")

            proveedores_list.append({
                "ID": proveedor['id'],
                "name": proveedor['name'],
                "location": proveedor['location']
            })
        

    # Crear el archivo JSON
    create_json_file('./../../1_data_bda/json/data_providers.json', proveedores_list)
    print("Archivo JSON generado correctamente.")

    create_csv_file('./../../1_data_bda/csv/data_providers.csv', proveedores_list)
    print("Archivo CSV generado correctamente.")

    # Cerrar la conexión con Neo4j
    neo4j_client.close()