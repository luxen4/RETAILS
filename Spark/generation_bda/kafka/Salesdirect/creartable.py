import psycopg2

def createTable_Sales_streem():
    try:
        connection = psycopg2.connect(host='localhost' , port='5432',database='retail_db', user= 'postgres', password='casa1234')
        cursor = connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_stream (
            sales_id SERIAL PRIMARY KEY,
            store_id INTEGER,
            product_id INTEGER,
            quantity_sold DECIMAL(10,2),
            revenue DECIMAL(10,2)  
        );
        """)
        
        connection.commit()
        print("Tabla sales_stream creada correctamente.")
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error:", e)

createTable_Sales_streem()

 
   
   
