from task_1_python_correction import *
from queries import *

#connection to DB, table and index creation, data load into tables
create_connection()
create_database()
execute_query(create_rooms_table)
execute_query(create_students_table)
load_data_rooms()
load_data_students()
execute_query(create_index_query)

#query execution
execute_read_query(query_1)
convert_to_json(execute_read_query(query_1))
execute_read_query(query_2)
convert_to_json(execute_read_query(query_2))
execute_read_query(query_3)
convert_to_json(execute_read_query(query_3))
execute_read_query(query_4)
convert_to_json(execute_read_query(query_4))


