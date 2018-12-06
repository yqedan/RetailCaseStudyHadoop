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

public class RetailAggReducer extends Reducer<Text, SaleRecordWritable, Text, Text> {
	private static final Logger retailLogger = Logger.getLogger(RetailAggMapper.class);
	
	private Map<String, String> promoMap = new HashMap<String, String>();

	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
	    File promoFile = new File("promotion.txt");
	    //File promoFile = new File("promotions_merged");
	    
	    FileInputStream fis = new FileInputStream(promoFile);
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
	protected void reduce(Text key, Iterable<SaleRecordWritable> sales , Context context)
			throws IOException, InterruptedException {	
		
		Double totalSalesWeekdays = 0.0;
		Double totalSalesWeekends = 0.0;
		String [] keyTokens = key.toString().split(",");
		
		String[] promoTokens = promoMap.get(keyTokens[1]).split(",");
		String salesYear = keyTokens[2];
		String salesMonth = keyTokens[3];
		String regionId = keyTokens[0];
		String promoId = keyTokens[1];
		String promoName = promoTokens[0];
		Double promoCost = Double.parseDouble(promoTokens[1]);
		
		for(SaleRecordWritable sale: sales){
			Double totalSales = sale.getSales().get();
			String dayOfWeek = sale.getDay().toString();
						
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
		totalSalesWeekdays = round(totalSalesWeekdays);
		totalSalesWeekends = round(totalSalesWeekends);
		promoCost = round(promoCost);
		context.write(new Text(salesYear + "," + salesMonth + "," + regionId + "," + promoId + "," + promoName + "," + promoCost + "," + totalSalesWeekdays + "," +  totalSalesWeekends), new Text());
	}
	
	private static double round(double value) {
	    long factor = (long) Math.pow(10, 2);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
}