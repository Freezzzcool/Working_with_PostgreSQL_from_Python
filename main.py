import psycopg2
from psycopg2.sql import SQL, Identifier


def del_all_table():
    with conn.cursor() as cur:
        cur.execute('''
        DROP TABLE phone_numbers;
        DROP TABLE clients;
        ''')
    conn.commit()

def create_db():
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS clients(
            client_id SERIAL PRIMARY KEY,
            name VARCHAR(40) NOT NULL,
            surname VARCHAR(40) NOT NULL,
            email VARCHAR(40) NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS phone_numbers(
            phone_number_id SERIAL PRIMARY KEY,
            phone_number CHAR(11),
            client_id INTEGER REFERENCES clients(client_id)
            );            
        ''')
    conn.commit()
    return print('Таблицы clients, phone_numbers созданы')

def add_new_client(name, surname, email):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO clients(name, surname, email)
        VALUES (%s, %s, %s)
        RETURNING (client_id, name, surname, email);
        ''', (name, surname, email))
        return cur.fetchone()

def add_number(client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO phone_numbers(client_id, phone_number)
        VALUES (%s, %s)
        RETURNING (phone_number);
        ''', (client_id, phone_number))
        return cur.fetchone()

def change_email_client(client_id, new_email):
    with conn.cursor() as cur:
        cur.execute('''
        UPDATE clients
        SET email = %s
        WHERE client_id = %s
        RETURNING (client_id, email);
        ''', (new_email, client_id))
        conn.commit()

def del_phone_number(client_id):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM phone_numbers
        WHERE client_id = %s;
        ''', (client_id,))
        conn.commit()

def del_client(client_id):
    del_phone_number(client_id)
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM clients
        where client_id = %s;
        ''', (client_id,))
        conn.commit()

def change_client(client_id, name=None, surname=None, email=None):
    with conn.cursor() as cur:
        arg_list = {'name': name, "surname": surname, 'email': email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL('''
                UPDATE clients 
                SET {} = %s 
                WHERE client_id = %s
                ''').format(Identifier(key)), (arg, client_id))
        cur.execute('''
            SELECT * FROM clients
            WHERE client_id = %s
            ''', (client_id,))
        conn.commit()

def find_client(name=None, surname=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        if name:
            cur.execute('''
                SELECT c.name, c.surname, c.email, pn.phone_number FROM clients AS c
                LEFT JOIN phone_numbers AS pn ON c.client_id = pn.client_id
                WHERE c.name = %s;
                ''', (name,))
            return print(cur.fetchone())
        elif surname:
            cur.execute('''
                SELECT c.name, c.surname, c.email, pn.phone_number FROM clients AS c
                LEFT JOIN phone_numbers AS pn ON c.client_id = pn.client_id
                WHERE c.surname = %s;
                ''', (surname,))
            return print(cur.fetchone())
        elif email:
            cur.execute('''
                SELECT c.name, c.surname, c.email, pn.phone_number FROM clients AS c
                LEFT JOIN phone_numbers AS pn ON c.client_id = pn.client_id
                WHERE c.email = %s;
                ''', (email,))
            return print(cur.fetchone())
        elif phone_number:
            cur.execute('''
                SELECT c.name, c.surname, c.email, pn.phone_number FROM clients AS c
                LEFT JOIN phone_numbers AS pn ON c.client_id = pn.client_id
                WHERE c.phone_number = %s;
                ''', (phone_number,))
            return print(cur.fetchone())

if __name__ == '__main__':
    with psycopg2.connect(database='', user='', password='') as conn:
        # del_all_table()
        #
        # create_db()
        #
        # add_new_client('Иван', 'Иванов', 'Ivanov@mail.ru')
        # add_new_client('Петр', 'Петров', 'Petrov@mail.ru')
        # 
        # add_number('1', '89008007766')
        # add_number('2', '89007004433')

        # change_client('1', name='Ваня')
        # change_email_client('1', 'IvanIvanov@mail.ru')

        # del_phone_number('1')

        # del_client('2')

        # find_client('Иван')

    conn.close()

