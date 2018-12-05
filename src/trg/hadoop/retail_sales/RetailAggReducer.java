package trg.hadoop.retail_sales;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class RetailAggReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger retailLogger = Logger.getLogger(RetailAggMapper.class);
	
	private Map<String, String> promoMap = new HashMap<String, String>();

	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
	    File prodFile = new File("promotion.txt");
	    FileInputStream fis = new FileInputStream(prodFile);
	    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		
		String line = reader.readLine();
		
		while(line != null) {
			String[] tokens = line.split(",");
			String promoID = tokens[0];
			String promoName = tokens[1];
			String promoCost = tokens[2];
			
			promoMap.put(promoID, promoName + "," + promoCost);
			line = reader.readLine();
		}
		reader.close();

		if (promoMap.isEmpty()) {
			throw new IOException("Unable to load Product data.");
		}
	}
	
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
		
		String[] promoTokens = promoMap.get(keyTokens[1]).split(",");
		
		context.write(new Text(keyTokens[2] + "," + keyTokens[3] + "," + keyTokens[0] + "," + keyTokens[1] + "," + promoTokens[0] + "," + promoTokens[1] + "," + totalSalesWeekdays.toString() + "," +  totalSalesWeekends.toString()), new Text());
	}
}