import json
import psycopg2.extras

#INSTALLING CONNECTION
hostname = 'localhost'
database = 'booking'
username = 'postgres'
pwd = 'pass'
port_id = 5433
conn = None
cur = None
myresult = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    #table creation rooms
    cur.execute('DROP TABLE IF EXISTS rooms')
    create_script ='''CREATE TABLE IF NOT EXISTS rooms(
                     id int PRIMARY KEY,
                     name varchar(100) NOT NULL)
                     '''
    cur.execute(create_script)

    # table creation students
    cur.execute('DROP TABLE IF EXISTS students')
    create_script_st = '''CREATE TABLE IF NOT EXISTS students(
                         birthday date,
                         id int PRIMARY KEY,
                         name varchar(100) NOT NULL,
                         room int NOT NULL REFERENCES rooms(id),
                         sex varchar(5))
                         '''
    cur.execute(create_script)
    cur.execute(create_script_st)

    #DATA LOAD FROM FILES TO TABLES
    with open("rooms.json", "r") as f:
        json_load = json.loads(f.read())
        for row in json_load:
            lst = [v for v in dict.values(row)]
            tpl_lst = tuple(lst)
            # print(tpl_lst)
            cur.execute('INSERT INTO rooms(id,name) VALUES (%s,%s)', tpl_lst)

    with open("students.json", "r") as f:
        json_load_2 = json.loads(f.read())
        for row_2 in json_load_2:
            lst_2 = [v for v in dict.values(row_2)]
            tpl_lst_2 = tuple(lst_2)
            cur.execute('INSERT INTO students(birthday,id,name,room,sex) VALUES (%s,%s,%s,%s,%s)', tpl_lst_2)

    #query_1
    cur.execute(
        "SELECT r.name AS room_name, COUNT(s.name) AS number_of_students FROM rooms r JOIN students s ON r.id=s.room GROUP BY room_name")
    results_query_1 = cur.fetchall()
    d = dict(results_query_1)
    lst = []
    for i in results_query_1:
        lst.append(dict(i))
    json_string = json.dumps(lst, indent=2)
    with open('query_1.json', 'w') as f:
        f.write(json_string)
        f.close

    #query_2
    cur.execute("SELECT r.name, EXTRACT(YEAR FROM AVG(AGE(s.birthday))) AS min_avg_age FROM rooms r JOIN students s ON r.id=s.room GROUP BY r.name ORDER BY min_avg_age LIMIT 5")
    results_query_2 = cur.fetchall()
    d2 = dict(results_query_2)
    lst_2 = []
    for i in results_query_2:
        lst_2.append(i)
    json_string_2 = json.dumps(lst_2, indent=2)
    with open('query_2.json', 'w') as f:
        f.write(json_string_2)
        f.close

    #query_3
    cur.execute("""SELECT room_name, MAX(age)-MIN(age) AS diff_age
                    FROM (SELECT r.name AS room_name, EXTRACT (YEAR FROM AGE(s.birthday)) AS age
                         FROM rooms r JOIN students s ON r.id=s.room) sub
                            GROUP BY room_name ORDER BY diff_age DESC LIMIT 5;""")
    results_3 = cur.fetchall()
    d3 = dict(results_3)
    lst_3 = []
    for i in results_3:
        lst_3.append(i)
    json_string_3 = json.dumps(lst_3, indent=2)
    with open('query_3.json', 'w') as f:
        f.write(json_string_3)
        f.close

    #query_4
    cur.execute("""select distinct room
                    from
                    (SELECT room,sex,
                    lead(sex) over(partition by room order by room) as sex_next
                    FROM students) sub
                    where sex!=sex_next""")
    result_query_4 = cur.fetchall()
    with open('query_4.txt', 'w', newline = '\n') as f:
        for i in result_query_4:
            f.write(str(i))
        f.close

   # Для двух таблиц (students, rooms) автоматически создаются кластеризованные индексы, сформированные вокруг первичного ключа.
   # При выполнении запросов по поиску данных и при условии, что данные будут отфильтрованы по первичному ключу, база данных поможет
   # быстро справиться с задачей и быстро вернет результат.
   # Нет смысла создавать индексы на колонки, по которым нет условия поиска (where, and/or, ‘=’, ‘>’,’’<).
   # В случае написанных запросов запрос с условием where есть только для 4-го вопроса. Для него создан индекс st_room.
   #  index_creation = 'CREATE INDEX st_room ON students(room);'

    #INDEX_CREATION
    cur.execute("""CREATE INDEX st_room ON students(room);""")

    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()




