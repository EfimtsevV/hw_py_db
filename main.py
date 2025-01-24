import psycopg2

def create_db(conn):
    with conn.cursor() as cursor:
        
        cursor.execute("""
        DROP TABLE IF EXISTS client_phones CASCADE;
        DROP TABLE IF EXISTS phones CASCADE;
        DROP TABLE IF EXISTS clients CASCADE;
        """)
        

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE
        );
        
        CREATE TABLE IF NOT EXISTS phones (
            id SERIAL PRIMARY KEY,
            phone VARCHAR(11) UNIQUE
        );
        
        CREATE TABLE IF NOT EXISTS client_phones (
            id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
            phone_id INTEGER REFERENCES phones(id) ON DELETE CASCADE
        );
        """)
        conn.commit()
        
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO clients (first_name, last_name, email)
        VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id;
        """, (first_name, last_name, email))
        
        client_id = cursor.fetchone()
        if client_id:
            client_id = client_id[0]
        else:
            cursor.execute("SELECT id FROM clients WHERE email = %s;", (email,))
            client_id = cursor.fetchone()[0]

        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)
        
        conn.commit()

        
def add_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO phones (phone)
        VALUES (%s) ON CONFLICT (phone) DO NOTHING RETURNING id;
        """, (phone,))
        phone_id = cursor.fetchone()
        if not phone_id:
            cursor.execute("SELECT id FROM phones WHERE phone = %s;", (phone,))
            phone_id = cursor.fetchone()[0]
        else:
            phone_id = phone_id[0]
        cursor.execute("""
        INSERT INTO client_phones (client_id, phone_id)
        VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """, (client_id, phone_id))
        conn.commit()
        
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM clients WHERE id = %s;", (client_id,))
        if not cursor.fetchone():
            print(f"Client with id {client_id} does not exist.")
            return  
        if first_name:
            cursor.execute("UPDATE clients SET first_name = %s WHERE id = %s;", (first_name, client_id))
        if last_name:
            cursor.execute("UPDATE clients SET last_name = %s WHERE id = %s;", (last_name, client_id))
        if email:
            cursor.execute("UPDATE clients SET email = %s WHERE id = %s;", (email, client_id))

        if phones is not None:
            cursor.execute("DELETE FROM client_phones WHERE client_id = %s;", (client_id,))
            for phone in phones:
                add_phone(conn, client_id, phone)
        conn.commit()

        
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT id FROM phones WHERE phone = %s;
        """, (phone,))
        phone_id = cursor.fetchone()
        if phone_id:
            phone_id = phone_id[0]
            cursor.execute("""
            DELETE FROM client_phones WHERE client_id = %s AND phone_id = %s;
            """, (client_id, phone_id))
            cursor.execute("""
            DELETE FROM phones WHERE id = %s;
            """, (phone_id,))
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM clients WHERE id = %s;", (client_id,))
        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cursor:
        find_query = """
        SELECT c.id, c.first_name, c.last_name, c.email, 
               STRING_AGG(p.phone, ', ') AS phones
        FROM clients c
        LEFT JOIN client_phones cp ON c.id = cp.client_id
        LEFT JOIN phones p ON cp.phone_id = p.id
        WHERE (%s IS NULL OR c.first_name = %s)
          AND (%s IS NULL OR c.last_name = %s)
          AND (%s IS NULL OR c.email = %s)
          AND (%s IS NULL OR p.phone = %s)
        GROUP BY c.id, c.first_name, c.last_name, c.email;
        """
        cursor.execute(find_query, (first_name, first_name, last_name, last_name, email, email, phone, phone))
        results = cursor.fetchall()
        for result in results:
            print(result)

            
if __name__ == "__main__":
    with psycopg2.connect(database="hw_pydb", user="postgres", password="12345") as conn:
        create_db(conn)
        add_client(conn, "Vlad", "Efimtsev", "efimtsevva@mail,ru", "89858440828")
        add_client(conn, "Egor", "Antonenko", "egor_antonenko@mail,ru", ["89265345678", "89152301818"])
        add_phone(conn, 1, "89858220202")
        change_client(conn, 2, first_name="Oleg", phones=["89265345677", "89152301717"])
        delete_phone(conn, 2, "89152301717")
        delete_client(conn, 2)
        find_client(conn, last_name="Efimtsev")
    conn.close()
