package trg.hadoop.retail_sales;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SaleRecordWritable implements WritableComparable<SaleRecordWritable>{
	
	private DoubleWritable sales;
	private Text the_day;
	private IntWritable the_year;
	private Text the_month;
	private IntWritable region_id;
	private IntWritable promotion_id;
	
	public SaleRecordWritable(){
		this.sales = new DoubleWritable();
		this.the_day = new Text();
		this.the_year = new IntWritable();
		this.the_month = new Text();
		this.region_id = new IntWritable();
		this.promotion_id = new IntWritable();
	}
	
	public void set(double sales, String the_day, int the_year, String the_month, int region_id, int promotion_id){
		this.sales.set(sales);
		this.the_day.set(the_day);
		this.the_year.set(the_year);
		this.the_month.set(the_month);
		this.region_id.set(region_id);
		this.promotion_id.set(promotion_id);
	}
	
	public DoubleWritable getSales(){
		return sales;
	}
	
	public Text getDay(){
		return the_day;
	}
	
	public IntWritable getYear(){
		return the_year;
	}
	
	public Text getMonth(){
		return the_month;
	}
	
	public IntWritable getRegionId(){
		return region_id;
	}
	
	public IntWritable getPromotionId(){
		return promotion_id;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		sales.readFields(in);
		the_day.readFields(in);
		the_year.readFields(in);
		the_month.readFields(in);
		region_id.readFields(in);
		promotion_id.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sales.write(out);
		the_day.write(out);
		the_year.write(out);
		the_month.write(out);
		region_id.write(out);
		promotion_id.write(out);
	}

	@Override
	public int compareTo(SaleRecordWritable o) {
		if (sales.compareTo(o.sales) == 0) {
			return region_id.compareTo(o.region_id);
		} else
			return sales.compareTo(o.sales);
	}
}
