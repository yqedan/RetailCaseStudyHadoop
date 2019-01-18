package trg.hadoop.retail_sales;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RetailAgg extends Configured implements Tool{
	
	enum RetailMessages {
		NO_PROMOTION
	}

@Override
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		
		job.setJobName(this.getClass().getName());
		
		job.setJarByClass(getClass());
		
	    try{
	        // DistributedCache.addCacheFile(new URI("/training/dc/product.txt#product.txt"), job.getConfiguration());
	    	//job.addCacheFile(new URI("/home/cloudera/workspace/RetailSales/data/promotion.txt#promotion.txt"));
	    	//job.addCacheFile(new URI("/user/cloudera/food_mart/promotions/promotions_merged"));
	    	job.addCacheFile(new URI("/user/yusuf9191/food_mart/promotions/promotions_merged"));
	    	
	        }catch(Exception e){
	        	System.out.println(e);
	        }
		
		
		 URI[] cacheFiles= job.getCacheFiles();
		 if(cacheFiles != null) {
			 for (URI cacheFile : cacheFiles) {
				 System.out.println("Cache file ->" + cacheFile);
			 }
		 } 	

		// configure output and input source
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		
		job.setMapperClass(RetailAggMapper.class);
		//job.setCombinerClass(RetailAggReducer.class);
		job.setReducerClass(RetailAggReducer.class);
		
		job.setNumReduceTasks(2);
		

		// configure output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SaleRecordWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path outputPath = new Path(args[1]);
		
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return job.waitForCompletion(true) ? 0 : 1;
}
public static void main(String[] args) throws Exception {	
		int exitCode = ToolRunner.run(new RetailAgg(), args);
		System.exit(exitCode);
	}
}
