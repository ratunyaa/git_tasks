import json
import os
import psycopg2.extras
from psycopg2 import OperationalError
from dotenv import load_dotenv

def create_connection(db_name, db_user, db_password, db_host, db_port):
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return conn

conn = create_connection(
    os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASS'), os.getenv('DB_HOST'), os.getenv('DB_PORT')
)

def create_database(conn, query):
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute('DROP DATABASE IF EXISTS booking')
        cur.execute(query)
        print("Database has been created")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

create_dtabase_query = "CREATE DATABASE booking"
create_database(conn, create_dtabase_query)

conn = create_connection(
    os.getenv('DB_NEW'), os.getenv('DB_USER'), os.getenv('DB_PASS'), os.getenv('DB_HOST'), os.getenv('DB_PORT'))

def execute_query(conn, query):
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

create_rooms_table ='''CREATE TABLE IF NOT EXISTS rooms(
                     id int PRIMARY KEY,
                     name varchar(100) NOT NULL)
                     '''
execute_query(conn, create_rooms_table)

create_students_table = '''CREATE TABLE IF NOT EXISTS students(
                         birthday date,
                         id int PRIMARY KEY,
                         name varchar(100) NOT NULL,
                         room int NOT NULL REFERENCES rooms(id),
                         sex varchar(5))
                         '''
execute_query(conn, create_students_table)


def convert(*args):
    """Returns a list of tuples generated from multiple lists and tuples"""
    for x in args:
        if not isinstance(x, list) and not isinstance(x, tuple):
            return []
    size = float("inf")
    for x in args:
        size = min(size, len(x))
    result = []
    for i in range(size):
        result.append(tuple([x[i] for x in args]))
    return result


with open("rooms.json", "r") as f:
    json_load = json.loads(f.read())
    id = []
    name = []
    i = 0
    for row in json_load:
        lst = [v for v in dict.values(row)]
        a = lst[0]
        b = lst[1]
        id.append(a)
        name.append(b)
        i+=1
result = convert(id, name)
rooms_records = ", ".join(["%s"] * len(result))

def load_data(conn, query):
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
insert_query = (f"INSERT INTO rooms (id, name) VALUES {rooms_records}")
conn.autocommit = True
cur = conn.cursor()
cur.execute(insert_query, result)

with open("students.json", "r") as f:
    json_load_2 = json.loads(f.read())
    birthday = []
    id = []
    name = []
    room = []
    sex = []
    i = 0
    for row in json_load_2:
        lst_2 = [v for v in dict.values(row)]
        a = lst_2[0]
        b = lst_2[1]
        c = lst_2[2]
        d = lst_2[3]
        e = lst_2[4]
        birthday.append(a)
        id.append(b)
        name.append(c)
        room.append(d)
        sex.append(e)
        i += 1
result2 = convert(birthday, id, name, room, sex)
students_records = ", ".join(["%s"] * len(result2))
insert_query2 = (f"INSERT INTO students (birthday,id,name,room,sex) VALUES {students_records}")
conn.autocommit = True
cur = conn.cursor()
cur.execute(insert_query2, result2)

#query_1
def execute_read_query(conn, query):
    cur = conn.cursor()
    result = None
    try:
        cur.execute(query)
        result = cur.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")

query_1 = "SELECT r.name AS room_name, COUNT(s.name) AS number_of_students FROM rooms r JOIN students s ON r.id=s.room GROUP BY room_name;"
query_1_res = execute_read_query(conn, query_1)

def convert_to_json(res):
    lst = []
    for i in res:
        lst.append(i)
    json_string = json.dumps(lst, indent=2)
    with open('query_result.json', 'w') as f:
        f.write(json_string)
        f.close()

convert_to_json(query_1_res)

#query_2
query_2 = "SELECT r.name, EXTRACT(YEAR FROM AVG(AGE(s.birthday))) AS min_avg_age " \
          "FROM rooms r JOIN students s ON r.id=s.room " \
          "GROUP BY r.name " \
          "ORDER BY min_avg_age " \
          "LIMIT 5"
query_2_res = execute_read_query(conn, query_2)

#load query results to file
convert_to_json(query_2_res)

#query_3
query_3 = "SELECT room_name, MAX(age)-MIN(age) AS diff_age " \
          "FROM (SELECT r.name AS room_name, EXTRACT (YEAR FROM AGE(s.birthday)) AS age " \
          "FROM rooms r JOIN students s ON r.id=s.room) sub " \
          "GROUP BY room_name ORDER BY diff_age DESC LIMIT 5;"
query_3_res = execute_read_query(conn, query_3)

#load query results to file
convert_to_json(query_3_res)

#query_4
query_4 = "select distinct room " \
          "from (SELECT room,sex, lead(sex) over(partition by room order by room) as sex_next " \
          "FROM students) sub where sex!=sex_next"
query_4_res = execute_read_query(conn, query_4)

#load query results to file
convert_to_json(query_4_res)


"""Для двух таблиц (students, rooms) автоматически создаются кластеризованные индексы, сформированные вокруг первичного ключа.
   При выполнении запросов по поиску данных и при условии, что данные будут отфильтрованы по первичному ключу, база данных поможет
   быстро справиться с задачей и быстро вернет результат.
   Нет смысла создавать индексы на колонки, по которым нет условия поиска (where, and/or, ‘=’, ‘>’,’’<).
   В случае написанных запросов запрос с условием where есть только для 4-го вопроса. Для него создан индекс st_room.
    index_creation = 'CREATE INDEX st_room ON students(room);'"""

#INDEX_CREATION
def create_index(conn, query):
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute(query)
        print("Index created successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

create_index_query = "CREATE INDEX st_room ON students(room)"
create_index(conn, create_index_query)
cur.close()




