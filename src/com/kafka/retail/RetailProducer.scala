package com.kafka.retail

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

object RetailProducer extends App{
	    val topic = "RetailSales"
			val brokers = "localhost:9092"
			val props = new Properties()
			props.put("bootstrap.servers", brokers)
			props.put("client.id", "RetailProducer")
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			val producer = new KafkaProducer[String, String](props)
			val spark = SparkSession.builder.enableHiveSupport.getOrCreate()
			import spark.implicits._
			val storeSalesFormattedDF = spark.sql("select * from sales.store_sales_formatted_spark")
			val storeSalesFormatedCollected = storeSalesFormattedDF.collect
			val storeSalesFormatedCollectedFirstRow = storeSalesFormattedDF.first
			var i = 0;
			storeSalesFormattedDF.foreach(x => {
			  i += 1
			  val rowString = x.getInt(0) + "," + x.getInt(1) + "," + x.getDouble(2) + "," + x.getDouble(3) + "," + x.getDouble(4)
			  val data = new ProducerRecord[String, String](topic, i.toString, rowString)
			  producer.send(data)  
			})
			producer.close
}