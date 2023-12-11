-- Task 1
SELECT country_code AS Country, count(distinct store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY 2 DESC;

-- Task 2
SELECT locality, count(distinct store_code) AS total_no_stores
FROM dim_store_details
GROUP BY 1
ORDER BY 2 DESC;


-- Task 3
SELECT 
	ROUND(CAST(sum(sd.product_price*ot.product_quantity) AS numeric),2) as total_price,
    dtt.month
FROM orders_table as ot 
LEFT JOIN dim_date_times as dtt
ON ot.date_uuid = dtt.date_uuid
LEFT JOIN sales_data as sd
ON ot.product_code = sd.product_code
GROUP BY dtt.month
ORDER BY 1 DESC;

-- Task 4
SELECT 
	count(sd.product_code) as numbers_of_sales,
	sum(product_quantity) as product_quantity,
--	dsd.store_type, 
	CASE 
		WHEN dsd.store_type = 'Web Portal' THEN 'Web'
		ELSE 'Offline'
	END as location
FROM orders_table as ot 

LEFT JOIN dim_date_times as dtt
ON ot.date_uuid = dtt.date_uuid

LEFT JOIN sales_data as sd
ON ot.product_code = sd.product_code

LEFT JOIN dim_store_details as dsd
ON ot.store_code = dsd.store_code
GROUP BY 3
ORDER BY 3 DESC;


-- Task 5
SELECT 
	dsd.store_type,
    ROUND(CAST(sum(product_quantity*sd.product_price) AS NUMERIC),2),
    ROUND(CAST((100*sum(product_quantity*sd.product_price)/
    sum(sum(product_quantity*sd.product_price)) over()) AS NUMERIC),2) as "percentage_total%"

	
FROM orders_table as ot 

LEFT JOIN dim_date_times as dtt
ON ot.date_uuid = dtt.date_uuid

LEFT JOIN sales_data as sd
ON ot.product_code = sd.product_code

LEFT JOIN dim_store_details as dsd
ON ot.store_code = dsd.store_code
GROUP BY 1
ORDER BY 2 DESC;

-- Task 6
(SELECT 
	ROUND(CAST(sum(product_quantity*sd.product_price) AS numeric),2),
	dtt.year,
 	dtt.month
FROM orders_table as ot 

LEFT JOIN dim_date_times as dtt
ON ot.date_uuid = dtt.date_uuid

LEFT JOIN sales_data as sd
ON ot.product_code = sd.product_code

LEFT JOIN dim_store_details as dsd
ON ot.store_code = dsd.store_code
GROUP BY 2,3
ORDER BY 1 DESC);

-- Task 7

SELECT sum(staff_numbers) as total_staff_numbers, country_code
FROM dim_store_details
GROUP BY 2
ORDER BY 1 DESC;

-- Task 8

(SELECT 
	ROUND(CAST(sum(ot.product_quantity*sd.product_price) AS numeric),2) as total_sales,
	dsd.store_type,
 	dsd.country_code
FROM orders_table as ot 

LEFT JOIN dim_date_times as dtt
ON ot.date_uuid = dtt.date_uuid

LEFT JOIN sales_data as sd
ON ot.product_code = sd.product_code

LEFT JOIN dim_store_details as dsd
ON ot.store_code = dsd.store_code
WHERE dsd.country_code = 'DE'
GROUP BY 2,3
ORDER BY 1);

-- Task 9

SELECT 
    year, 
    CONCAT('"hours": ',H,'"minutes": ',M,'"seconds": ',S,'"miliseconds": ',S) -- as actual_time_taken
    FROM
    (SELECT 
        year, 
        AVG(hours),
        FLOOR(AVG(hours)) as H, 
        FLOOR((3600* AVG(hours) % 3600 / 60)) as M,
        FLOOR((3600* AVG(hours)% 3600 % 60)) as S,
        FLOOR(((3600* AVG(hours)% 3600 % 60 )*1000 %1000)) as MS
        FROM
        (SELECT 
            year,
            month,
            day,
            timestamp,
            concat(year,'-',month,'-',day,' ',timestamp) as full_timestamp, 
            LEAD(concat(year,'-',month,'-',day,' ',timestamp),1) over (order by year, month, day, timestamp) as shifted_timestamp,
            (EXTRACT(EPOCH FROM (LEAD(concat(year,'-',month,'-',day,' ',timestamp),1) over (order by year, month, day, timestamp))::timestamp)--)
            - EXTRACT(EPOCH FROM concat(year,'-',month,'-',day,'- ',timestamp) ::timestamp)) as difference,
            -- HOURS
            ((EXTRACT(EPOCH FROM (LEAD(concat(year,'-',month,'-',day,' ',timestamp),1) over (order by year, month, day, timestamp))::timestamp)--)
            - EXTRACT(EPOCH FROM concat(year,'-',month,'-',day,'- ',timestamp) ::timestamp)) / 3600) AS hours,
            -- MINUTES
            ((EXTRACT(EPOCH FROM (LEAD(concat(year,'-',month,'-',day,' ',timestamp),1) over (order by year, month, day, timestamp))::timestamp)--)
            - EXTRACT(EPOCH FROM concat(year,'-',month,'-',day,'- ',timestamp) ::timestamp)) % 3600) / 60 AS minutes,
            -- SECONDS
            ((EXTRACT(EPOCH FROM (LEAD(concat(year,'-',month,'-',day,' ',timestamp),1) over (order by year, month, day, timestamp))::timestamp)--)
            - EXTRACT(EPOCH FROM concat(year,'-',month,'-',day,'- ',timestamp) ::timestamp)) % 3600 % 60) AS seconds--)
            -- ,DATE_DIFF(SECOND,)
            -- OVER (PARTITION BY YEAR) -- AS actual_time_taken
            FROM dim_date_times
            ORDER BY 1,2,3,4,5)
            GROUP BY 1
            ORDER BY 2 DESC);