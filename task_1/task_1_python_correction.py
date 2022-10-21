import os
import psycopg2.extras
from psycopg2 import OperationalError
import pandas as pd


def create_connection():
    """creates connection to database"""
    try:
        connection = psycopg2.connect(
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASS'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
        print("Connection to database has been installed successfully")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")

conn = create_connection()
conn.autocommit = True
cur = conn.cursor()


def create_database():
    """creation of database booking"""
    try:
        cur.execute('DROP DATABASE IF EXISTS booking')
        cur.execute("CREATE DATABASE booking")
        print("Database has been created")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_query(query):
    """sql query execution for table creation"""
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


create_rooms_table = '''CREATE TABLE IF NOT EXISTS rooms(
                     id int PRIMARY KEY,
                     name varchar(100) NOT NULL)
                     '''

create_students_table = '''CREATE TABLE IF NOT EXISTS students(
                         birthday date,
                         id int PRIMARY KEY,
                         name varchar(100) NOT NULL,
                         room int NOT NULL REFERENCES rooms(id),
                         sex varchar(5))
                         '''


def load_data_rooms():
    """load data into table rooms"""
    conn.autocommit = True
    cur = conn.cursor()
    try:
        file = open('rooms.json', 'r')
        df = pd.read_json(file, orient='records')
        df_rooms = list(df.itertuples(index=False, name=None))
        rooms_records = ", ".join(["%s"] * len(df_rooms))
        insert_query = (f"INSERT INTO rooms (id, name) VALUES {rooms_records}")
        cur.execute(insert_query, df_rooms)
        print("Data has been successfully loaded into rooms table")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    cur.close()


def load_data_students():
    """load data into table students"""
    conn.autocommit = True
    cur = conn.cursor()
    try:
        file = open('students.json', 'r')
        df1 = pd.read_json(file, orient='records')
        df_students = list(df1.itertuples(index=False, name=None))
        students_records = ", ".join(["%s"] * len(df_students))
        insert_query_students = (f"INSERT INTO students (birthday,id,name,room,sex) VALUES {students_records}")
        cur.execute(insert_query_students, df_students)
        print("Data has been successfully loaded into students table")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    cur.close()


"""Для двух таблиц (students, rooms) автоматически создаются кластеризованные индексы, сформированные вокруг первичного ключа.
   При выполнении запросов по поиску данных и при условии, что данные будут отфильтрованы по первичному ключу, база данных поможет
   быстро справиться с задачей и быстро вернет результат.
   Нет смысла создавать индексы на колонки, по которым нет условия поиска (where, and/or, ‘=’, ‘>’,’’<).
   В случае написанных запросов запрос с условием where есть только для 4-го вопроса. Для него создан индекс st_room.
    index_creation = 'CREATE INDEX st_room ON students(room);'"""

#INDEX_CREATION
create_index_query = "CREATE INDEX st_room ON students(room)"


# entry point

def main():
    create_connection()
    create_database()
    execute_query(create_rooms_table)
    execute_query(create_students_table)
    load_data_rooms()
    load_data_students()
    execute_query(create_index_query)


if __name__ == "__main__":
    main()
else:
    print(f"main: {__name__}")








