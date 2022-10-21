import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import psycopg2.extras

load_dotenv()

def create_connection():
    """creates connection"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT"))
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return cur
cur = create_connection()

def spark_session():
    session = SparkSession \
        .builder.master("local") \
        .appName("PySpark PostgreSQL") \
        .config("spark.jars", "/Users/ratunya/postgresql-42.2.6.jar") \
        .config("spark.driver.extraClasspath", "/Users/ratunya/spark-3.3.0-bin-hadoop3/jars/postgresql-42.2.6.jar") \
        .config("spark.executor.extraClassPath", "/Users/ratunya/spark-3.3.0-bin-hadoop3/jars/postgresql-42.2.6.jar") \
        .getOrCreate()
    return session

spark = spark_session()

def create_actor():
    """creation of table actor"""
    cur.execute(
        "SELECT * FROM actor")
    results_query_1 = cur.fetchall()
    schema = ["actor_id", "first_name", "last_name", "last_update"]
    df_act = spark.createDataFrame(data = results_query_1, schema = schema)
    return df_act

df_actor = create_actor()

def create_film():
    """creation of table film"""
    cur.execute(
        "SELECT film_id,title,description,release_year,language_id,rental_duration,\
         rental_rate,length,replacement_cost,rating,last_update,special_features,fulltext FROM film")
    film = cur.fetchall()
    schema_film = ["film_id", "title", "description","release_year","language_id","rental_duration","rental_rate","length",
                "replacement_cost","rating","last_update","special_features","fulltext"]
    df_flm = spark.createDataFrame(data=film, schema=schema_film)
    return df_flm

df_film = create_film()

def create_film_category():
    """creation of table film_category"""
    cur.execute(
        "SELECT film_id,category_id FROM film_category"
    )
    film_category = cur.fetchall()
    schema_film_category = ["film_id", "category_id"]
    df_fcategory = spark.createDataFrame(data = film_category, schema=schema_film_category)
    return df_fcategory

df_film_category = create_film_category()


def create_category():
    """creation of table category"""
    cur.execute(
        "SELECT category_id, name FROM category"
    )
    category = cur.fetchall()
    schema_category = ["category_id", "name"]
    df_cat = spark.createDataFrame(data = category, schema=schema_category)
    return df_cat

df_category = create_category()

def create_film_actor():
    """creation of table film_actor"""
    cur.execute("SELECT actor_id, film_id FROM film_actor")
    film_actor = cur.fetchall()
    schema_film_actor = ["actor_id", "film_id"]
    df_fa = spark.createDataFrame(data = film_actor, schema = schema_film_actor)
    return df_fa

df_film_actor = create_film_actor()


def create_inventory():
    """creation of table inventory"""
    cur.execute("SELECT inventory_id, film_id, store_id FROM inventory")
    inventory = cur.fetchall()
    schema_inventory = ["inventory_id", "film_id", "store_id"]
    df_inv = spark.createDataFrame(data = inventory, schema = schema_inventory)
    return df_inv

df_inventory = create_inventory()

def create_rental():
    """creation of table rental"""
    cur.execute("SELECT * FROM rental")
    rental = cur.fetchall()
    schema_rental = ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "satff_id", "last_update"]
    df_rent = spark.createDataFrame(data = rental, schema = schema_rental)
    return df_rent

df_rental = create_rental()

def create_payment():
    """creation of table payment"""
    cur.execute("SELECT * FROM PAYMENT")
    payment = cur.fetchall()
    schema_payment = ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"]
    df_pay = spark.createDataFrame(data = payment, schema=schema_payment)
    return df_pay

df_payment = create_payment()

def create_customer():
    """creation of table customer"""
    cur.execute("SELECT * FROM customer")
    customer = cur.fetchall()
    schema_customer = ["customer_id", "store_id", "first_name", "last_name", "email", "address_id",
                       "activebool", "create_date", "last_update", "active"]
    df_cust = spark.createDataFrame(data = customer, schema=schema_customer)
    return df_cust

df_customer = create_customer()

def create_store():
    """creation of table store"""
    cur.execute("SELECT * FROM store")
    store = cur.fetchall()
    schema_store = ["store_id", "manager_staff_id", "address_id", "last_update"]
    df_st = spark.createDataFrame(data = store, schema=schema_store)
    return df_st

df_store = create_store()

def create_address():
    """creation of table address"""
    cur.execute("SELECT * FROM address")
    address = cur.fetchall()
    schema_address = ["address_id", "address", "address2", "district",
                      "city_id", "postal_code", "phone", "last_update"]
    df_addr = spark.createDataFrame(data=address, schema=schema_address)
    return df_addr

df_address = create_address()

def create_city():
    """creation of table city"""
    cur.execute("SELECT * FROM city")
    city = cur.fetchall()
    schema_city = ["city_id", "city", "country_id", "last_update"]
    df_ct = spark.createDataFrame(data=city, schema=schema_city)
    return df_ct

df_city = create_city()

def main():
    create_connection()
    spark_session()
    create_actor()
    create_film()
    create_film_category()
    create_category()
    create_film_actor()
    create_inventory()
    create_rental()
    create_payment()
    create_customer()
    create_store()
    create_address()
    create_city()

if __name__ == "__main__":
    main()
else:
    print(f"main: {__name__}")
