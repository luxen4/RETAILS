from py2neo import Graph

# Conexión con la base de datos Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "your_password"  # Cambiar por tu contraseña

try:
    graph = Graph(uri, auth=(user, password))

    # Consultar los nodos de los productos
    query = """
    MATCH (product:Product)
    RETURN product
    """
    result = graph.run(query)

    # Imprimir los resultados
    print("Productos en la base de datos:")
    for record in result:
        print(record["product"])

except Exception as e:
    print(f"Error al consultar los productos en la base de datos: {e}")