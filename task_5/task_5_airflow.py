from datetime import datetime
import pymongo
from airflow.decorators import dag, task
import pandas as pd


# [start instantiate_dag]
@dag(schedule_interval=None,
     start_date=datetime(2021, 1, 1),
     catchup=False,
     tags=['task_etl'])
def task_etl():

    @task()
    def extract():
        """ Extract task.
        A task to load data from csv file, which is stored locally.
        """
        file = pd.read_csv('/Users/ratunya/airflow/dags/tiktok_reviews.csv', skip_blank_lines=True)
        return file

    @task()
    def transform(file):
        """Transform task.
        A task to make data modifications with file that is transferred from Extract task:
        - replaces NA values with '-',
        - removes unnecessary symbols from 'content'-column (leaves text and punctuation only)
        - sorts values by date of creation in descending order
        """
        file.fillna('-', inplace=True)
        file.sort_values(by=['at'], inplace=True, ascending=False)
        file['content'].str.replace('\W', ' ', regex=True)
        file['content'] = file['content'].str.replace('[^A-Za-z0-9]', ' ', regex=True)
        file['content'].replace('\d+', '', regex=True, inplace=True)
        dt = pd.DataFrame(file)
        return dt

    @task()
    def load(dt):
        """
        #### Load task
        A Load task which takes in the result of the Transform task and
        loads the data into MongoDB database
        """
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["tiktokdata"]
        mycol = mydb["task5"]
        data = dt.to_dict(orient='records')
        x = mycol.insert_many(data)
        print('loaded successfully')


    extract_data = extract()
    transform_data = transform(extract_data)
    load_data = load(transform_data)

    # [end main_flow]


# [start dag_invocation]
etl_dag_draft = task_etl()
# [end dag_invocation]



