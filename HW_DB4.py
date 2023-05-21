import psycopg2


def delete_tables():
    cur.execute("""
    DROP TABLE IF EXISTS phone;
    DROP TABLE IF EXISTS client;    
    """)
    conn.commit()


def create_tables():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client(
        id SERIAL PRIMARY KEY,
        firstname VARCHAR(40) NOT NULL,
        lastname VARCHAR(40) NOT NULL,
        mail VARCHAR(40) NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS phone(
        id SERIAL PRIMARY KEY,
        client_id INT NOT NULL REFERENCES client(id),
        phone_number BIGINT NOT NULL UNIQUE
    );
    """)
    conn.commit()


def add_phone_to_client(client_id, phone_number=None):
    print(f'Добавление нового телефона: ID клиента = {client_id}, Телефон = {phone_number}')
    cur.execute("""
    INSERT INTO phone (client_id, phone_number) VALUES
    (%s, %s) RETURNING client_id, phone_number;
    """, (client_id, phone_number))
    client_id, phone_number = cur.fetchall()[0]
    print(f'Клиенту {client_id} добавлен номер телефона {phone_number}')
    print()


def add_client(firstname, lastname, mail, phones=None):
    print(f'Добавление нового клиента: Имя = {firstname}, Фамилия = {lastname}, Почта = {mail}, Телефоны = {phones}')
    cur.execute("""
    INSERT INTO client (firstname, lastname, mail) VALUES
    (%s, %s, %s)
    RETURNING id, firstname, lastname, mail;
    """, (firstname, lastname, mail))
    client_id, firstname, lastname, mail = cur.fetchall()[0]
    print(f'Добавлен клиент ID_{client_id} {firstname} {lastname} {mail}')
    print()
    if phones:
        for phone_number in phones:
            add_phone_to_client(client_id, phone_number)


def search_client(client_id=None, firstname=None, lastname=None, mail=None, phones=None):
    phones = phones if phones else [None]
    for phone_number in phones:
        cur.execute("""
        SELECT client.id FROM client
        JOIN phone ON client.id = phone.client_id
        WHERE client.id=%s OR firstname=%s OR lastname=%s OR mail=%s OR phone.phone_number = %s
        """, (client_id, firstname, lastname, mail, phone_number))
        print(
            f'Поиск: ID клиента = {client_id}, Имя = {firstname}, Фамилия = {lastname}, Почта = {mail}, Телефон = {phone_number}')
        result = sorted({int(*i) for i in cur.fetchall()})
        print(f'ID клиентов, соответствующих поиску: {result}', end='\n\n')
        return result


def client_info(client_id):
    try:
        cur.execute("""
        SELECT client.id, firstname, lastname, mail, phone_number FROM client
        LEFT JOIN phone ON client.id = phone.client_id
        WHERE client.id = %s
        """, (client_id,))
        print(f'Данные клиента:')
        phones = []
        for result in cur.fetchall():
            client_id, firstname, lastname, mail, phone_number = result
            phones += [phone_number]
        print(f'''ID клиента: {client_id}
Имя: {firstname}
Фамилия: {lastname}
Почта: {mail}
Телефоны: {' '.join(map(str, phones))}''', end='\n\n')
    except:
        print(f'Клиента с ID {client_id} в базе не существует')


def edit_client_request(client_id, column_name, value):
    cur.execute(f"""
    UPDATE client SET {column_name}=%s WHERE id=%s;
    """, (value, client_id))
    cur.execute("""
    SELECT * FROM client;
    """)
    print(f'Изменение данных клиента. ID клиента = {client_id}. {column_name} = {value}.')
    print(*cur.fetchall()[-1], end='\n\n')


def edit_client(client_id, firstname=None, lastname=None, mail=None, phones=None):
    if firstname:
        edit_client_request(client_id, 'firstname', firstname)
    if lastname:
        edit_client_request(client_id, 'lastname', lastname)
    if mail:
        edit_client_request(client_id, 'mail', mail)
    if phones:
        delete_phones(client_id)
        for phone_number in phones:
            add_phone_to_client(client_id, phone_number)


def delete_phones(client_id):
    print(f'Удаление всех телефонов у клиента: ID клиента = {client_id}')
    cur.execute("""
    DELETE FROM phone WHERE client_id=%s;
    """, (client_id,))
    conn.commit()
    print()


def delete_client(client_id):
    print(f'Удаление клиента из базы: ID клиента = {client_id}')
    delete_phones(client_id)
    cur.execute("""
    DELETE FROM client WHERE id=%s;
    """, (client_id,))
    conn.commit()


if __name__ == '__main__':
    with psycopg2.connect(database='netology_db', user='postgres', password='postgres') as conn:
        with conn.cursor() as cur:
            delete_tables()
            create_tables()
            add_client('Ivan', 'Ivanov', 'ivan@mail.ru', [89051234567, 89011234567, 89999876543])
            add_client('Petr', 'Petrov', 'petr@mail.ru')
            add_client('First', 'F', 'ff@mail.ru', [11111, 22222, 33333])
            add_client('Second', 'S', 'ss@mail.ru', [2])
            add_client('Third', 'S', 'ts@mail.ru', [3])
            add_client(firstname='Sidr', lastname='Sidorov', mail='sidr@mail.ru', phones=[87444444])
            add_phone_to_client(1, 1111111111111111)
            add_phone_to_client(1, 2222222222222222)
            add_phone_to_client(3, 898989898989899)
            search_client(client_id=1)
            search_client(firstname='Petr')
            search_client(mail='ivan@mail.ru')
            search_client(lastname='bbbbb')
            search_client(phones=[3])
            search_client(phones=[22222])
            search_client(phones=[89999876543, 3])
            search_client(lastname='S')
            client_info(2)
            client_info(3)
            edit_client(client_id=1, firstname='Vasya')
            edit_client(6, 'Dima', 'Dimanov', 'dima@mail.ru')
            edit_client(6, 'Dima', 'Dimanov', 'dima@mail.ru', [555555555555555555])
            client_info(6)
            delete_phones(1)
            client_info(1)
            delete_phones(4)
            client_info(4)
            delete_phones(5)
            client_info(5)
            delete_client(3)
            client_info(3)
    conn.close()
