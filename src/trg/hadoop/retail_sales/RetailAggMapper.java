package trg.hadoop.retail_sales;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import trg.hadoop.retail_sales.RetailAgg.RetailMessages;

public class RetailAggMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Logger retailLogger = Logger.getLogger(RetailAggMapper.class);
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		
		String region_id = tokens[0];
		Integer promotion_id = Integer.parseInt(tokens[3]);
		String the_year = tokens[7];
		String the_month = tokens[6];
		String sales = tokens[4];
		String the_day = tokens[5]; 
		
		if(promotion_id == 0){
			context.getCounter(RetailMessages.NO_PROMOTION).increment(1);
			//retailLogger.error("No promotion found : " + value.toString());
		}else{
			context.write(new Text(region_id + "," + promotion_id +  "," + the_year + ","  + the_month), new Text(sales + "," + the_day + "," + the_year + "," + the_month + "," + region_id + "," + promotion_id));
		}

	}
}
