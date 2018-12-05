package trg.hadoop.retail_sales;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class RetailAggReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger retailLogger = Logger.getLogger(RetailAggMapper.class);
	@Override
	protected void reduce(Text key, Iterable<Text> sales , Context context)
			throws IOException, InterruptedException {	
		
		Double totalSalesWeekdays = 0.0;
		Double totalSalesWeekends = 0.0;
		String [] keyTokens = key.toString().split(",");
		
		for(Text sale: sales){
			String[] tokens = sale.toString().split(",");
			Double totalSales = Double.parseDouble(tokens[0]);
			String dayOfWeek = tokens[1].toString();
						
			switch(dayOfWeek){
				case "Monday":
					totalSalesWeekdays = totalSalesWeekdays + totalSales;
					break;
				case "Tuesday":
					totalSalesWeekdays = totalSalesWeekdays + totalSales;
					break;
				case "Wednesday":
					totalSalesWeekdays = totalSalesWeekdays + totalSales;
					break;
				case "Thursday":
					totalSalesWeekdays = totalSalesWeekdays + totalSales;
					break;
				case "Friday":
					totalSalesWeekdays = totalSalesWeekdays + totalSales;
					break;
				case "Saturday":
					totalSalesWeekends = totalSalesWeekends + totalSales;
					break;
				case "Sunday":		
					totalSalesWeekends = totalSalesWeekends + totalSales;
					break;
				default:
					retailLogger.error("day of week is in the wrong format");
			}
		}
		
		context.write(new Text(keyTokens[2] + "," + keyTokens[3] + "," + keyTokens[0] + "," + keyTokens[1] + "," + totalSalesWeekdays.toString() + "," +  totalSalesWeekends.toString()), new Text());
	}
}