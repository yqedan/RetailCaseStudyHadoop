// NOTE all of this is tested and runs in the spark-shell version 2.2 with a spark session already set up

// Load data into Hive table and execute Queries in Spark (using scala)

// c. Write the following queries using Apache Spark and save the output in another hive table:

// Query1: List the total weekday sales & weekend sales for each promotions:
// Region ID, Promotion ID, Promotion Cost, total weekday sales, total weekend sales

val storeSalesDF = spark.sql("select * from sales.store_sales_part")
val storeSalesDFformatted = storeSalesDF.select("region_id","promotion_id","promotion_cost","weekday_sales","weekend_sales")
storeSalesDFformatted.createOrReplaceTempView("store_sales_formatted_view")

// Create table in hive first:
// NOTE make sure your hive metastore is configured to be in the same location for hive and Spark for this to work

/*
create table store_sales_formatted_spark(region_id int, promotion_id int, promotion_cost double, weekday_sales double, weekend_sales double)
row format delimited
fields terminated by ','
stored as textfile;
*/

spark.sql("insert into sales.store_sales_formatted_spark select * from store_sales_formatted_view")

// Query 2: List the promotions, which generated highest total sales (weekday + weekend) in
// each region. Following columns are required in output:
// Region ID, Promotion ID, Promotion Cost, total sales

val regionSales = storeSalesDFformatted.groupBy("region_id","promotion_id","promotion_cost").agg(sum("weekday_sales").plus(sum("weekend_sales")).name("total_sales"))
val regionHighestSales = regionSales.groupBy("region_id").agg(first("promotion_id").name("promotion_id"),first("promotion_cost").name("promotion_cost"),max("total_sales").name("max_total_sales"))
regionHighestSales.createOrReplaceTempView("store_sales_aggregated_view")

// Create table in hive first:

/*
create table store_sales_aggregated_spark(region_id int, promotion_id int, promotion_cost double, total_sales double)
row format delimited
fields terminated by ','
stored as textfile;
*/

spark.sql("insert into sales.store_sales_aggregated_spark select * from store_sales_aggregated_view")
