--TASK_1
SELECT c.name AS category_name, COUNT(f.title) AS number_of_films
FROM film f INNER JOIN film_category fc ON f.film_id=fc.film_id 
INNER JOIN category c ON fc.category_id = c.category_id
GROUP BY category_name
ORDER BY number_of_films DESC;

--TASK_2
SELECT a.first_name ||' '|| a.last_name AS full_name, COUNT(r.rental_id) AS number_of_rentals
FROM actor a INNER JOIN film_actor fa ON a.actor_id = fa.actor_id 
INNER JOIN film f ON fa.film_id = f.film_id 
INNER JOIN inventory i ON i.inventory_id  = f.film_id 
INNER JOIN rental r ON i.inventory_id = r.inventory_id 
GROUP BY full_name
ORDER BY number_of_rentals DESC
LIMIT 10;


--in cases of the same number of rentals within the range of 10
WITH cte2 AS (SELECT a.first_name ||' '|| a.last_name AS full_name, 
COUNT(r.rental_id) AS number_of_rentals,
dense_rank() OVER (ORDER BY COUNT(r.rental_id) DESC) AS rnk
FROM actor a INNER JOIN film_actor fa ON a.actor_id = fa.actor_id 
INNER JOIN film f ON fa.film_id = f.film_id 
INNER JOIN inventory i ON i.inventory_id  = f.film_id 
INNER JOIN rental r ON i.inventory_id = r.inventory_id 
GROUP BY full_name
ORDER BY number_of_rentals DESC)

SELECT full_name FROM cte2 
WHERE rnk<=10;

--TASK_3
--Вывести категорию фильмов, на которую потратили больше всего денег.
SELECT c."name", SUM(p.amount) AS revenue
FROM category c 
INNER JOIN film_category fc ON c.category_id = fc.category_id 
INNER JOIN film f ON f.film_id = fc.film_id 
INNER JOIN inventory i ON i.film_id = f.film_id 
INNER JOIN rental r ON i.inventory_id = r.inventory_id 
INNER JOIN payment p ON p.rental_id = r.rental_id 
GROUP BY c."name"
ORDER BY revenue DESC
LIMIT 1;



WITH cte3 AS (SELECT c."name", 
DENSE_RANK() OVER (ORDER BY SUM(p.amount) DESC) AS rnk
FROM category c 
INNER JOIN film_category fc ON c.category_id = fc.category_id 
INNER JOIN film f ON f.film_id = fc.film_id 
INNER JOIN inventory i ON i.film_id = f.film_id 
INNER JOIN rental r ON i.inventory_id = r.inventory_id 
INNER JOIN payment p ON p.rental_id = r.rental_id 
GROUP BY c."name"
ORDER BY rnk ASC)

SELECT name FROM cte3 WHERE rnk=1;

--TASK_4 
--Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
SELECT f.title                        --, f.film_id AS film_id_ex, i.film_id AS film_id_inv
FROM category c 
INNER JOIN film_category fc ON c.category_id = fc.category_id 
INNER JOIN film f ON f.film_id = fc.film_id 
LEFT JOIN inventory i ON i.film_id = f.film_id
WHERE i.film_id IS NULL;

--TASK_5
--Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
--Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
WITH cte5 AS (SELECT c."name" AS category_name, a.first_name || ' '|| a.last_name AS full_name,
COUNT(f.title) AS film_number_per_category,
DENSE_RANK() OVER(ORDER BY COUNT(f.title) DESC) AS rnk
FROM film f
INNER JOIN film_actor fa ON f.film_id = fa.film_id 
INNER JOIN actor a ON fa.actor_id = a.actor_id 
INNER JOIN film_category fc ON fc.film_id = f.film_id 
INNER JOIN category c ON fc.category_id = c.category_id 
GROUP BY category_name, full_name
HAVING c."name" = 'Children')

SELECT full_name AS top_3_actors FROM cte5 WHERE rnk<=3;

--TASK_6
--Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
--Отсортировать по количеству неактивных клиентов по убыванию.
SELECT c2.city,
CASE WHEN c.active = 1 THEN COUNT(c.customer_id) ELSE 0 END AS active_customers,
CASE WHEN c.active = 0 THEN COUNT(c.customer_id) ELSE 0 END AS non_active_customers
FROM customer c
INNER JOIN address a ON a.address_id = c.address_id 
INNER JOIN city c2 ON c2.city_id = a.city_id 
GROUP BY  c2.city,c.active
ORDER BY non_active_customers DESC;

-- TASK_7
--Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
-- и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

WITH ct AS (SELECT c2.city, c3.name AS category_name, SUM(DATE_PART('day', return_date::timestamp - rental_date::timestamp)) AS durability
FROM rental r 
INNER JOIN customer c ON r.customer_id = c.customer_id 
INNER JOIN address a ON a.address_id = c.address_id 
INNER JOIN city c2 ON a.city_id = c2.city_id 
INNER JOIN inventory i ON i.inventory_id = r.inventory_id 
INNER JOIN film f ON i.film_id = f.film_id 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
INNER JOIN category c3 ON c3.category_id = fc.category_id 
WHERE city LIKE 'A%' OR city LIKE '%-%' 
GROUP BY category_name,c2.city
ORDER BY durability DESC),

ct2 AS (SELECT city, category_name, durability FROM ct WHERE city LIKE '%-%' AND durability IS NOT NULL  ORDER BY durability DESC LIMIT 1),
ct3 AS (SELECT city, category_name, durability FROM ct WHERE city LIKE 'A%' AND durability IS NOT NULL ORDER BY durability DESC LIMIT 1)


SELECT * FROM ct3
UNION ALL 
SELECT * FROM ct2;



















