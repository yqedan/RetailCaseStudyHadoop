package trg.hadoop.retail_sales;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
}
public static void main(String[] args) throws Exception {
		File dir = new File("/home/cloudera/workspace/RetailSales/invout");
		
		if(dir.isDirectory() == false) {
			System.out.println("Not a directory. Do nothing");
		}else{
			File[] listFiles = dir.listFiles();
			for(File file : listFiles){
				System.out.println("Deleting "+file.getName());
				file.delete();
			}
			//now directory is empty, so we can delete it
			System.out.println("Deleting Directory. Success = "+dir.delete());
		}
		
		int exitCode = ToolRunner.run(new RetailAgg(), args);
		System.exit(exitCode);
	}
}
