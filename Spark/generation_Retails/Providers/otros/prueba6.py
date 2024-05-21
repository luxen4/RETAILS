from pyspark.sql import SparkSession

# Ruta al archivo JAR del conector Neo4j-Spark
neo4j_spark_connector_jar = "neo4j-connector-apache-spark_2.13-5.3.0_for_spark_3.jar"

# Crear una instancia de SparkSession con la configuración del JAR
spark = SparkSession.builder \
    .appName("Neo4jSparkIntegration") \
    .config("spark.jars", neo4j_spark_connector_jar) \
    .getOrCreate()

# Configuración de conexión a Neo4j
neo4j_config = {
    "neo4j.uri": "bolt://localhost:7687",
    "neo4j.authentication.basic.username": "neo4j",
    "neo4j.authentication.basic.password": "your_password"
}

# Consulta Cypher para recuperar nodos de productos
query = "MATCH (p:Product) RETURN p"

# Leer datos desde Neo4j a Spark DataFrame
neo4j_df = spark.read.format("org.neo4j.spark.DataSource") \
    .option("query", query) \
    .options(**neo4j_config) \
    .load()

# Mostrar el DataFrame resultante
neo4j_df.show()