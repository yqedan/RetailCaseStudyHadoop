-- Load data into Hive table and execute Queries in Spark (using scala)

-- a. Read the JSon / CSV file using a Hive External Table

create external table store_sales (sales_year int, sales_month string, region_id int, promotion_id int, promotion_name string, promotion_cost double, weekday_sales double, weekend_sales double)
row format delimited
fields terminated by ","
stored as textfile
location "/user/cloudera/food_mart/sales_map_reduce_out/invout";

-- b. Create and load a Hive Partitioned table partitioned by region_id, sales_year, sales_month

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- NOTE: sometimes you need to increase the amount of max partitions from 100 otherwise the job will fail, this is set by the following parameters. I set them to really high amounts to be on the safe side.
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;

create table store_sales_part_parquet (promotion_id int, promotion_name string, promotion_cost double, weekday_sales double, weekend_sales double)
partitioned by (region_id int, sales_year int, sales_month string)
stored as parquet;

-- NO NOT do select * as it will order the columns wrong when copying! Column order is very important.
insert into store_sales_part_parquet
partition (region_id, sales_year, sales_month)
select promotion_id, promotion_name, promotion_cost, weekday_sales, weekend_sales, region_id, sales_year, sales_month from store_sales;

-- If saving table as parquet fails because your computer cant keep up try CSV instead:

create table store_sales_part (promotion_id int, promotion_name string, promotion_cost double, weekday_sales double, weekend_sales double)
partitioned by (region_id int, sales_year int, sales_month string)
row format delimited
fields terminated by ','
stored as textfile;

insert into store_sales_part
partition (region_id, sales_year, sales_month)
select promotion_id, promotion_name, promotion_cost, weekday_sales, weekend_sales, region_id, sales_year, sales_month from store_sales;
