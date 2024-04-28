import psycopg2
from pprint import pprint

# удаление таблиц personaldata и phones
def drop_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE personaldata;
        DROP TABLE phones;
        """)

# создание таблиц personaldata и phones
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE phones(
                phone_id SERIAL PRIMARY KEY,
                number BIGINT NOT NULL
                );
            """)
        cur.execute("""
            CREATE TABLE personaldata(
                personal_id SERIAL PRIMARY KEY,
                firstname VARCHAR(100) NOT NULL,
                lastname VARCHAR(100) NOT NULL,
                email VARCHAR(150),
                phone_id INTEGER references phones(phone_id)
                );
            """)

# добавление нового клиента в таблицы
def add_new_person(conn, firstname, lastname, email, phones=None):
    with conn.cursor() as cur:
        if phones:
            cur.execute("""
                INSERT INTO phones (number) VALUES
                    (%s)
                    RETURNING phone_id;
                """, (phones,))
            ph_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO personaldata (firstname, lastname, email, phone_id) VALUES
                    (%s, %s, %s, %s); 
                """, (firstname, lastname, email, ph_id))
        else:
            cur.execute("""
                INSERT INTO personaldata (firstname, lastname, email) VALUES
                    (%s, %s, %s); 
                """, (firstname, lastname, email))

# добавление телефона клиенту, если уже был в базе -> создает новую запись в personaldata с новым телефоном
def add_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
            select firstname, lastname, email, phone_id from personaldata
            where personal_id = %s;
            """, (client_id,))
        personal = cur.fetchone()
        fn, ln, email = personal[0], personal[1], personal[2]
        if personal[3] != None:
            add_new_person(conn, fn, ln, email, number)
        else:
            cur.execute("""
                INSERT INTO phones (number) VALUES
                    (%s)
                    RETURNING phone_id;
                """, (number,))
            ph_id = cur.fetchone()[0]
            cur.execute("""
                update personaldata set phone_id = %s
                where personal_id = %s;
                """, (ph_id, client_id))

# проверка на наличие номера и создание его если нет, если есть обновление существующего + проверка номера на None
def chk_update_phone(conn, client_id, phones):
    with conn.cursor() as cur:
        cur.execute("""
            select phone_id from personaldata
            where personal_id = %s;
            """, (client_id,))
        ph_id = cur.fetchone()[0]
        if ph_id == None:
            add_phone(conn, client_id, phones)
        else:
            if phones == None:
                print('Номер не может быть None!')
                return True
            else:
                cur.execute("""
                    UPDATE phones 
                    SET number = %s
                    WHERE  phone_id = %s;
                    """, (phones, ph_id))

# Редактировение клиента в зависимости от входных аргументов в функцию.
def edit_client(conn, client_id, firstname=None, lastname=None, email=None, phones=None):
    elements = [firstname, lastname, email, phones]
    with conn.cursor() as cur:
        if elements == [None, None, None, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
        elif elements == [None, None, email, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET email = %s
                    WHERE  personal_id = %s;
                    """, (email, client_id))
        elif elements == [None, lastname, None, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET lastname = %s
                    WHERE  personal_id = %s;
                    """, (lastname, client_id))
        elif elements == [None, lastname, email, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET lastname = %s,
                        email = %s
                    WHERE  personal_id = %s;
                    """, (lastname, email, client_id))
        elif elements == [firstname, None, None, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s
                    WHERE  personal_id = %s;
                    """, (firstname, client_id))
        elif elements == [firstname, None, email, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s,
                        email = %s
                    WHERE  personal_id = %s;
                    """, (firstname, email, client_id))
        elif elements == [firstname, lastname, None, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s,
                        lastname = %s
                    WHERE  personal_id = %s;
                    """, (firstname, lastname, client_id))
        elif elements == [firstname, lastname, email, phones]:
            if chk_update_phone(conn, client_id, phones) == True:
                print('End')
            else:
                cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s,
                        lastname = %s,
                        email = %s
                    WHERE  personal_id = %s;
                    """, (firstname, lastname, email, client_id))
        elif elements == [None, None, email, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET email = %s
                    WHERE  personal_id = %s;
                    """, (email, client_id))
        elif elements == [None, lastname, None, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET lastname = %s
                    WHERE  personal_id = %s;
                    """, (lastname, client_id))
        elif elements == [None, lastname, email, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET lastname = %s,
                        email = %s
                    WHERE  personal_id = %s;
                    """, (lastname, email, client_id))
        elif elements == [firstname, None, None, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s
                    WHERE  personal_id = %s;
                    """, (firstname, client_id))
        elif elements == [firstname, None, email, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s,
                        email = %s
                    WHERE  personal_id = %s;
                    """, (firstname, email, client_id))
        elif elements == [firstname, lastname, None, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s,
                        lastname = %s
                    WHERE  personal_id = %s;
                    """, (firstname, lastname, client_id))
        elif elements == [firstname, lastname, email, None]:
            cur.execute("""
                    UPDATE personaldata 
                    SET firstname = %s,
                        lastname = %s,
                        email = %s
                    WHERE  personal_id = %s;
                    """, (firstname, lastname, email, client_id))

# удаление телефона у клиента
def del_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT phone_id from personaldata 
            where personal_id = %s;
            """, (client_id,))
        ph_id = cur.fetchone()[0]
        cur.execute("""
            update personaldata set phone_id = NULL
            where personal_id = %s;
            """, (client_id,))
        cur.execute("""
            delete from phones
            where phone_id = %s and number = %s;
            """, (ph_id, number))

# удаление клиента
def del_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            select firstname, lastname, email, number from personaldata p
            left join phones p2 on p.phone_id = p2.phone_id
            where p.personal_id  = %s;
            """, (client_id,))
        obj = cur.fetchone()
        if len(obj) == 4:
            del_phone(conn, client_id, obj[3])
            cur.execute("""
                delete from personaldata
                where personal_id = %s;
                """, (client_id,))
        else:
            cur.execute("""
                delete from personaldata
                where personal_id = %s;
                """, (client_id,))

# Поиск клиента по входным аргументам в функцию.
def find_client(conn, firstname=None, lastname=None, email=None, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            select personal_id, firstname, lastname, email, number from personaldata p
            left join phones p2 on p.phone_id = p2.phone_id
            where firstname = %s or lastname = %s or email = %s or number = %s;
            """, (firstname, lastname, email, phones))
        pprint(cur.fetchall())


with psycopg2.connect(database='personalsdb', user='postgres', password='InnoTech') as conn:
    # create_tables(conn)
    # drop_table(conn)
    # add_new_person(conn, '111', '111', '111@mail.ru', 11111111)
    # add_new_person(conn, '222', '222', '222@mail.ru')
    # add_new_person(conn, '333', '333', '333@mail.ru')
    # add_new_person(conn, '555', '555', '555@mail.ru')
    # add_phone(conn, 2, 12345)
    # edit_client(conn, 3, 'new', None, 'new_email@ya.ru', 849512312322)
    # edit_client(conn, 1, 'one', None, 'one@ya.ru', 101010101)
    # del_phone(conn, 2, 12345)
    # find_client(conn, '222', None, None, None)
    # find_client(conn, None, None, None, 101010101)
    del_client(conn, 1)
conn.close()
