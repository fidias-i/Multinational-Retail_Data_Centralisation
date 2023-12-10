
-- Task 1: orders table chanes

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(22);
ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);
ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(12);
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Task 2: dim_users table changes
select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='dim_users';

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2);
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::date;

-- Task 3: dim_store_details changes
select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='dim_store_details';

UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat);
ALTER TABLE dim_store_details
DROP COLUMN lat;

UPDATE dim_store_details SET longitude = NULL WHERE longitude IN ('N/A','NULL');
UPDATE dim_store_details SET latitude = NULL WHERE latitude IN ('N/A','NULL');
UPDATE dim_store_details SET staff_numbers = NULL WHERE staff_numbers IN ('N/A','NULL');

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;
ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::date;
ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);
ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;
ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);
ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

-- Task 4: dim_products changes
-- The product_price column has a £ character 
-- which you need to remove using SQL.
select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='sales_data';


UPDATE sales_data
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE sales_data
ADD COLUMN weight_class VARCHAR(50);

ALTER TABLE sales_data
ALTER COLUMN weight TYPE FLOAT;

UPDATE sales_data
SET weight_class = 
    CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight < 40 and weight >=2 THEN 'Mid_sized'
        WHEN weight < 140 and weight >=40 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
        ELSE 'Unknown'
    END;


-- Task 5: dim_products changes
ALTER TABLE sales_data
ALTER COLUMN product_price TYPE FLOAT
USING product_price::double precision;

ALTER TABLE sales_data
ALTER COLUMN "EAN" TYPE VARCHAR(20);
ALTER TABLE sales_data
ALTER COLUMN product_code TYPE VARCHAR(20);
ALTER TABLE sales_data
ALTER COLUMN date_added TYPE DATE USING date_added::date;
ALTER TABLE sales_data
ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

ALTER TABLE sales_data
RENAME COLUMN removed TO still_available;

SELECT DISTINCT product_price 
FROM sales_data
WHERE product_price LIKE '%.%';

UPDATE sales_data
SET date_added = REPLACE(date_added, ' October ', '-10-');


UPDATE sales_data
SET still_available = 
    CASE
        WHEN still_available = 'Still_avaliable' THEN 1
        WHEN still_available = 'Removed' THEN 0
    END;


ALTER TABLE sales_data
ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;

-- Task 6: dim_date_times changes
select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='dim_date_times';

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(20)
ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);
ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(20);
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(20);
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- Task 7: 
select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='dim_card_details';

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

-- Task 8: Add primary keys
ALTER TABLE dim_card_details 
ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times 
ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_store_details 
ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users 
ADD PRIMARY KEY (user_uuid);

-- Task 9
select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME='orders_table';

-- violates foreign key constraint - card number Key (card_number)=(4971858637664481)
--  is not present in table "dim_card_details".
ALTER TABLE orders_table
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
-- run ok
ALTER TABLE orders_table
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
-- violates foreign key constraint: Key (store_code)=(WEB-1388012W)
-- is not present in table "dim_store_details"
ALTER TABLE orders_table
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
-- FIXED after replacing GBB to GB violates foreign key constraint
ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
