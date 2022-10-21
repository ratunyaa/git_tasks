from task_1_python_correction import *

def execute_read_query(query: str):
    """executes an sql query"""
    cur = conn.cursor()
    try:
        cur.execute(query)
        result = cur.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")

def convert_to_json(res: list):
    """converts a query result into json file"""
    try:
        convert_df = pd.DataFrame(res)
        json_file = convert_df.to_json('query_result.json', indent=2, orient='records')
        print("Data was loaded to json file")
        return json_file
    except OperationalError as e:
        print(f"The error '{e}' occurred")


query_1 = "SELECT r.name AS room_name, " \
          "COUNT(s.name) AS number_of_students " \
          "FROM rooms r JOIN students s " \
          "ON r.id=s.room GROUP BY room_name;"

query_2 = "SELECT r.name, EXTRACT(YEAR FROM AVG(AGE(s.birthday))) AS min_avg_age " \
          "FROM rooms r JOIN students s ON r.id=s.room " \
          "GROUP BY r.name " \
          "ORDER BY min_avg_age " \
          "LIMIT 5"

query_3 = "SELECT room_name, MAX(age)-MIN(age) AS diff_age " \
          "FROM (SELECT r.name AS room_name, EXTRACT (YEAR FROM AGE(s.birthday)) AS age " \
          "FROM rooms r JOIN students s ON r.id=s.room) sub " \
          "GROUP BY room_name ORDER BY diff_age DESC LIMIT 5;"

query_4 = "select distinct room " \
          "from (SELECT room,sex, lead(sex) over(partition by room order by room) as sex_next " \
          "FROM students) sub where sex!=sex_next"


def main():
    execute_read_query(query_1)
    convert_to_json(execute_read_query(query_1))
    execute_read_query(query_2)
    convert_to_json(execute_read_query(query_2))
    execute_read_query(query_3)
    convert_to_json(execute_read_query(query_3))
    execute_read_query(query_4)
    convert_to_json(execute_read_query(query_4))

if __name__ == "__main__":
    main()
else:
    print(f"main: {__name__}")