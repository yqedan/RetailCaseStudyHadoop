package com.kafka.retail

import java.util.Collections
import java.util.Properties

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.ConnectionFactory;

object RetailConsumer extends App{
	val topic = "RetailSales"
			val brokers = "localhost:9092"
			val groupId = "group01"
			val props = new Properties()
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
			props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
			props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
			val consumer = new KafkaConsumer[String, String](props)
			consumer.subscribe(Collections.singletonList(topic))
			//			val con = HBaseConfiguration.create();
			//			val admin = new HBaseAdmin(con);
			//			val logs = TableName.valueOf("logs");
			//			val tableDescriptor = new HTableDescriptor(logs);
			//			tableDescriptor.addFamily(new HColumnDescriptor("Log"));
			//      admin.createTable(tableDescriptor);
			val config = HBaseConfiguration.create();
    	var connection :Connection = null;
    	var table :Table = null;

	connection = ConnectionFactory.createConnection(config)
			table = connection.getTable(TableName.valueOf("logs"))

			var rowID = 0L
			while(true){
				val records = consumer.poll(1000)
						for (record <- records) {
							println("Received message: (" + record.key() + ", " + record.value + ") at offset " + record.offset())
							val tokens = record.value.split(",")
							println(tokens(0))
							//var testLog = Array("rowname","regionid","promotionid","promocost","weekday","weekend")
							val log = new Put(Bytes.toBytes(rowID.toString))
							log.addColumn(Bytes.toBytes("Log"), Bytes.toBytes("Region ID"), Bytes.toBytes(tokens(0)))
							log.addColumn(Bytes.toBytes("Log"), Bytes.toBytes("Promotion ID"), Bytes.toBytes(tokens(1)))
							log.addColumn(Bytes.toBytes("Log"), Bytes.toBytes("Promotion Cost"), Bytes.toBytes(tokens(2)))
							log.addColumn(Bytes.toBytes("Log"), Bytes.toBytes("Weekday Sales"), Bytes.toBytes(tokens(3)))
							log.addColumn(Bytes.toBytes("Log"), Bytes.toBytes("Weekend Sales"), Bytes.toBytes(tokens(4)))
							table.put(log)
							rowID += 1
						}		
			}
}