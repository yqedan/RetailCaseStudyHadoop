-- Load data into HDFS using SQOOP

-- a. Load retail data from foodmart database into MySQL.

-- b. Extract following columns from Promotion table in Food mart DB using SQOOP.
-- PromotionID, Promotion Name, Promotion Cost

-- c. Extract following columns from Sales_Fact_1997 & Sales_Fact_1998 tables in Food mart DB using SQOOP.
-- Region_id, ProductID, StoreID, PromotionID, Store Sales, the_day (day of week), the_month, the_year

-- MySQL queries and sqoop commands for a, b, and c in assignment.

create database food_mart

-- Load the food mart sql file int MySQL using the command:
-- $ mysql -u root -proot food_mart < foodmart_mysql.sql

select promotion_id, promotion_name, cost from promotion;

-- Ingest promotions table using sqoop command
-- $ sqoop import --connect jdbc:mysql://quickstart.cloudera/food_mart --username root --password cloudera --table promotion --columns promotion_id,promotion_name,cost --target-dir /user/cloudera/food_mart/promotions --m 2

create view sales_fact_all as select * from sales_fact_1998  union select * from sales_fact_1997;

create view sales_fact_all_joined as
select st.region_id, sf.product_id, sf.store_id, sf.promotion_id, sf.store_sales, tbd.the_day, tbd.the_month, tbd.the_year
from sales_fact_all as sf
inner join store as st
on st.store_id = sf.store_id
inner join time_by_day as tbd
on tbd.time_id = sf.time_id

-- Ingest sales table using sqoop command
-- $ sqoop import --connect jdbc:mysql://quickstart.cloudera/food_mart --username root --password cloudera --table sales_fact_all_joined --target-dir /user/cloudera/food_mart/sales_fact_all_joined --split-by store_id --m 2
