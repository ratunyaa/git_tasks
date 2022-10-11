from pyspark.sql import SparkSession
import psycopg2.extras
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when, expr, datediff

#INSTALLING CONNECTION
from pyspark.sql.functions import col, concat, lit, dense_rank, window

from env import hostname_v, database_v, username_v, pwd_v

hostname = hostname_v
database = database_v
username = username_v
pwd = pwd_v
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

    spark = SparkSession \
        .builder.master("local") \
        .appName("PySpark PostgreSQL") \
        .config("spark.jars", "/Users/ratunya/postgresql-42.2.6.jar") \
        .config("spark.driver.extraClasspath", "/Users/ratunya/spark-3.3.0-bin-hadoop3/jars/postgresql-42.2.6.jar") \
        .config("spark.executor.extraClassPath", "/Users/ratunya/spark-3.3.0-bin-hadoop3/jars/postgresql-42.2.6.jar") \
        .getOrCreate()

    # table actor
    cur.execute(
        "SELECT * FROM actor")
    results_query_1 = cur.fetchall()
    schema = ["actor_id", "first_name", "last_name", "last_update"]
    df_actor = spark.createDataFrame(data = results_query_1, schema = schema)

    #film
    cur.execute(
        "SELECT film_id,title,description,release_year,language_id,rental_duration,\
         rental_rate,length,replacement_cost,rating,last_update,special_features,fulltext FROM film")
    film = cur.fetchall()
    schema_film = ["film_id", "title", "description","release_year","language_id","rental_duration","rental_rate","length",
                "replacement_cost","rating","last_update","special_features","fulltext"]
    df_film = spark.createDataFrame(data=film, schema=schema_film)

    # film_category
    cur.execute(
        "SELECT film_id,category_id FROM film_category"
    )
    film_category = cur.fetchall()
    schema_film_category = ["film_id", "category_id"]
    df_film_category = spark.createDataFrame(data = film_category, schema=schema_film_category)

    #category
    cur.execute(
        "SELECT category_id, name FROM category"
    )
    category = cur.fetchall()
    schema_category = ["category_id","name"]
    df_category = spark.createDataFrame(data = category, schema=schema_category)

    #film_actor
    cur.execute("SELECT actor_id, film_id FROM film_actor")
    film_actor = cur.fetchall()
    schema_film_actor = ["actor_id", "film_id"]
    df_film_actor = spark.createDataFrame(data = film_actor, schema = schema_film_actor)

    #inventory
    cur.execute("SELECT inventory_id, film_id, store_id FROM inventory")
    inventory = cur.fetchall()
    schema_inventory = ["inventory_id", "film_id", "store_id"]
    df_inventory = spark.createDataFrame(data = inventory, schema = schema_inventory)

    #rental
    cur.execute("SELECT * FROM rental")
    rental = cur.fetchall()
    schema_rental = ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "satff_id", "last_update"]
    df_rental = spark.createDataFrame(data = rental, schema = schema_rental)

    #payment
    cur.execute("SELECT * FROM PAYMENT")
    payment = cur.fetchall()
    schema_payment = ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"]
    df_payment = spark.createDataFrame(data = payment, schema=schema_payment)

    #customer
    cur.execute("SELECT * FROM customer")
    customer = cur.fetchall()
    schema_customer = ["customer_id", "store_id", "first_name", "last_name", "email", "address_id",
                       "activebool", "create_date", "last_update", "active"]
    df_customer = spark.createDataFrame(data = customer, schema=schema_customer)

    #store
    cur.execute("SELECT * FROM store")
    store = cur.fetchall()
    schema_store = ["store_id", "manager_staff_id", "address_id", "last_update"]
    df_store = spark.createDataFrame(data = store, schema=schema_store)

    # address
    cur.execute("SELECT * FROM address")
    address = cur.fetchall()
    schema_address = ["address_id", "address", "address2", "district",
                      "city_id", "postal_code", "phone", "last_update"]
    df_address = spark.createDataFrame(data=address, schema=schema_address)

    #city
    cur.execute("SELECT * FROM city")
    city = cur.fetchall()
    schema_city = ["city_id", "city", "country_id", "last_update"]
    df_city = spark.createDataFrame(data=city, schema=schema_city)

    # TASK_1
    a = df_film.join(df_film,["film_id"]) \
           .join(df_film_category,df_film["film_id"] == df_film_category["film_id"]) \
           .join(df_category,df_category["category_id"] == df_film_category["category_id"]) \
           .groupBy("name").count().withColumnRenamed("count", "number_of_films").orderBy(col("count").desc()).show()

    # TASK_2
    b = df_actor.join(df_actor.alias("a"), ["actor_id"]) \
        .join(df_film_actor.alias("fa"), df_actor["actor_id"] == df_film_actor["actor_id"]) \
        .join(df_film, df_film["film_id"] == df_film_actor["film_id"]) \
        .join(df_inventory.alias("i"), df_inventory["film_id"] == df_film["film_id"]) \
        .join(df_rental.alias("r"), df_inventory["inventory_id"] == df_rental["inventory_id"])
    b.select(concat("a.first_name",lit(" "), "a.last_name") \
             .alias("Full_Name"), "r.rental_id")\
             .groupBy("Full_Name")\
             .count().withColumnRenamed("count", "number_of_rentals")\
             .orderBy(col("count").desc()) \
             .show(10)

    #TASK_3
    c = df_category.alias("c").join(df_film_category.alias("fc"), df_category["category_id"] == df_film_category["category_id"]) \
                   .join(df_film.alias("f"), df_film_category["film_id"] == df_film["film_id"]) \
                   .join(df_inventory.alias("i"), df_film["film_id"] == df_inventory["film_id"]) \
                   .join(df_rental.alias("r"), df_inventory["inventory_id"] == df_rental["inventory_id"]) \
                   .join(df_payment.alias("p"), df_rental["rental_id"] == df_payment["rental_id"])

    cc = c.select("c.name", "p.amount") \
        .groupBy("c.name")\
        .sum("p.amount")\
        .withColumnRenamed("sum(amount)", "revenue")

    cc.select("name")\
        .orderBy(col("revenue").desc()).show(1)

    #TASK_4
    s = df_category\
        .join(df_film_category.alias("fc"), df_film_category["category_id"] == df_category["category_id"])\
        .join(df_film.alias("f"), df_film["film_id"] == df_film_category["film_id"])\
        .join(df_inventory.alias("i"), df_inventory["film_id"] == df_film["film_id"], "left")
    s.select("f.title").filter("i.film_id IS NULL").show(s.count(), False)


    #TASK_5
    y = df_category.alias("c") \
        .join(df_film_category.alias("fc"), df_film_category["category_id"] == df_category["category_id"])\
        .join(df_film.alias("f"), df_film_category["film_id"] == df_film["film_id"])\
        .join(df_film_actor.alias("fa"), df_film_actor["film_id"] == df_film["film_id"])\
        .join(df_actor.alias("a"), df_film_actor["actor_id"] == df_actor["actor_id"])
    yy = y.select("fa.actor_id", concat("a.first_name", lit(" "), "a.last_name").alias("full_name"), "f.title",
                  "c.name")\
        .groupBy("full_name", "c.name").count()

    partition = Window.partitionBy("name").orderBy(col('count').desc())
    tsk = yy.withColumn("DENSE RANK", dense_rank().over(partition)).\
        filter("name == 'Children'")\
        .withColumnRenamed("DENSE RANK", 'rnk')
    tsk.select("full_name","rnk").filter("rnk in('1','2','3')").show(yy.count(), False)

    #TASK_7
    f = df_category.alias("cat")\
        .join(df_film_category.alias("fc"), df_category["category_id"] == df_film_category["category_id"])\
        .join(df_film.alias("f"), df_film["film_id"] == df_film_category["film_id"])\
        .join(df_inventory.alias("i"), df_inventory["film_id"] == df_film["film_id"])\
        .join(df_rental.alias("r"), df_inventory["inventory_id"] == df_rental["inventory_id"])\
        .join(df_customer.alias("cust"), df_rental["customer_id"] == df_customer["customer_id"])\
        .join(df_address.alias("adr"), df_customer["address_id"] == df_address["address_id"])\
        .join(df_city.alias("ct"), df_city["city_id"] == df_address["city_id"])\
        .filter(col("ct.city").like("A%") | col("ct.city").like("%-%"))

    g = f.select("*", datediff(col("return_date"), col("rental_date")).alias("durability"))
    f_max1 = g.select("cat.name", "ct.city", "durability").filter(col("ct.city").like("A%"))\
        .groupby("cat.name", "ct.city")\
        .sum("durability")
    df1 = f_max1.select("*").orderBy(col("sum(durability)").desc()).limit(1)

    f_max = g.select("cat.name", "ct.city", "durability").filter(col("ct.city").like("%-%")) \
        .groupby("cat.name", "ct.city") \
        .sum("durability")
    df2 = f_max.select("*").orderBy(col("sum(durability)").desc()).limit(1)

    df1.union(df2).withColumnRenamed("sum(durability)", "durability").show()

    #TASK_6
    t = df_customer.alias("c")\
        .join(df_store.alias("st"), df_store["store_id"] == df_customer["store_id"])\
        .join(df_address.alias("a"), df_address["address_id"] == df_store["address_id"])\
        .join(df_city.alias("ct"), df_city["city_id"] == df_address["city_id"])
    tt = t.select("c.active", "ct.city").filter("c.active == 1")\
        .groupBy("ct.city").count().withColumnRenamed("count","active_customers").show()
    tn = t.select("c.active", "ct.city").filter("c.active == 0") \
        .groupBy("ct.city").count().withColumnRenamed("count", "non_active_customers").show()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()