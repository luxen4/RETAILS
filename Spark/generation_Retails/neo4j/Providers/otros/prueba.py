from neo4j import GraphDatabase

# Conexión con la base de datos Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "your_password"  # Cambiar por tu contraseña

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        result = session.run("RETURN 1 AS result")
        for record in result:
            print(record["result"])
except Exception as e:
    print(f"Error al conectar con Neo4j: {e}")
finally:
    driver.close()  # Cerrar la conexión después de su uso
    
    
    

from py2neo import Graph, Node

# Conexión con la base de datos Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "your_password"  # Cambiar por tu contraseña

# Definir los nombres y propiedades de los productos
product1_data = {"name": "Product1", "price": 10.99}
product2_data = {"name": "Product2", "price": 20.49}

try:
    graph = Graph(uri, auth=(user, password))

    # Crear nodos de productos
    product1 = Node("Product", **product1_data)
    product2 = Node("Product", **product2_data)

    # Crear relaciones o propiedades adicionales si es necesario
    # Por ejemplo, product1["category"] = "Electronics"

    # Añadir los nodos de productos a la base de datos
    tx = graph.begin()
    tx.create(product1)
    tx.create(product2)
    tx.commit()

    print("Productos añadidos correctamente a la base de datos.")
except Exception as e:
    print(f"Error al añadir productos a la base de datos: {e}")