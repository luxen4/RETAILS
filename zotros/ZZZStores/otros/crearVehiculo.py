import psycopg2
import random
import string

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def create_table():
    try:
        conn = psycopg2.connect( host="localhost",database="retail_db",user="postgres", password="casa1234", port='5432' )
        createTableString='''CREATE TABLE if not exists vehiculo ( 
                            vehiculo_id SERIAL PRIMARY KEY, 
                            ref VARCHAR(255) NOT NULL, 
                            matricula VARCHAR(255) NOT NULL, 
                            bastidor VARCHAR(255) NOT NULL);'''
        
        
        with conn.cursor() as cur:
            cur.execute(createTableString)
        conn.commit()
        

        with conn.cursor() as cur:
            for x in range(1000):
                
                ref = get_random_string(4)
                matricula = get_random_string(4)
                bastidor = get_random_string(5)
                insertString = """INSERT INTO vehiculo (ref, matricula, bastidor) VALUES (%s, %s, %s)"""
                data_to_insert = (ref, matricula, bastidor)

                cur.execute(insertString, data_to_insert)
            conn.commit()
                
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_table()