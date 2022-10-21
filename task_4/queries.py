from task_4 import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when, expr, datediff
from pyspark.sql.functions import col, concat, lit, dense_rank, window

def task1_result():
    """
    Outputs number of films grouped by category, sorts by descending order.
    """
    a = df_film.join(df_film,["film_id"]) \
           .join(df_film_category,df_film["film_id"] == df_film_category["film_id"]) \
           .join(df_category,df_category["category_id"] == df_film_category["category_id"]) \
           .groupBy("name").count().withColumnRenamed("count", "number_of_films").orderBy(col("count").desc()).show()
    return a

def task2_result():
    """
    Outputs and sorts by descending order 10 actors by the most popular films rented.
    """
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
    return b

def task3_result():
    """
    Outputs a film category with the best revenue
    """
    c = df_category.alias("c").join(df_film_category.alias("fc"), df_category["category_id"] == df_film_category["category_id"]) \
                   .join(df_film.alias("f"), df_film_category["film_id"] == df_film["film_id"]) \
                   .join(df_inventory.alias("i"), df_film["film_id"] == df_inventory["film_id"]) \
                   .join(df_rental.alias("r"), df_inventory["inventory_id"] == df_rental["inventory_id"]) \
                   .join(df_payment.alias("p"), df_rental["rental_id"] == df_payment["rental_id"])

    cc = c.select("c.name", "p.amount") \
        .groupBy("c.name")\
        .sum("p.amount")\
        .withColumnRenamed("sum(amount)", "revenue")

    return cc.select("name").orderBy(col("revenue").desc()).show(1)


def task4_result():
    """
    Outputs film names that don't appear in inventory.
    """
    s = df_category\
        .join(df_film_category.alias("fc"), df_film_category["category_id"] == df_category["category_id"])\
        .join(df_film.alias("f"), df_film["film_id"] == df_film_category["film_id"])\
        .join(df_inventory.alias("i"), df_inventory["film_id"] == df_film["film_id"], "left")
    return s.select("f.title").filter("i.film_id IS NULL").show(s.count(), False)


def task5_result():
    """
    Outputs top-3 the most popular actors appeared in film category “Children”.
    """
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
    return tsk.select("full_name","rnk").filter("rnk in('1','2','3')").show(yy.count(), False)

def task6_result():
    """
    Outputs and sorts by desc order cities with number of active and non-active clients
    """
    t = df_customer.alias("c")\
        .join(df_store.alias("st"), df_store["store_id"] == df_customer["store_id"])\
        .join(df_address.alias("a"), df_address["address_id"] == df_store["address_id"])\
        .join(df_city.alias("ct"), df_city["city_id"] == df_address["city_id"])
    tt = t.select("c.active", "ct.city").filter("c.active == 1")\
        .groupBy("ct.city").count().withColumnRenamed("count","active_customers").show()
    tn = t.select("c.active", "ct.city").filter("c.active == 0") \
        .groupBy("ct.city").count().withColumnRenamed("count", "non_active_customers").show()
    return tn

def task7_result():
    """
    Outputs a film category with the highest amount of total summary rent in cities started with
    "a" and "-".
    """
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

    return df1.union(df2).withColumnRenamed("sum(durability)", "durability").show()


def main():
    task1_result()
    task2_result()
    task3_result()
    task4_result()
    task5_result()
    task6_result()
    task7_result()


if __name__ == "__main__":
    main()
else:
    print(f"main: {__name__}")