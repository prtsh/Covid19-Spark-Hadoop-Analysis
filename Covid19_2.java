import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.lang.Long;
import java.text.ParseException;
import java.util.Date;

//import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;

/*
Your second task is to modify your program to report total number of deaths for
every location/country in between a given range of dates.
*/

public class Covid19_2 {
	// input key(object), input value(record), output key(country), output value(covid cases)
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
	private LongWritable cases = new LongWritable(0);
    private Text country   = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(!value.toString().contains("date")){
				String[] tok = value.toString().split(",");
				Configuration conf = context.getConfiguration();
				try{
					Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(conf.get("start_date"));
					Date enddate   = new SimpleDateFormat("yyyy-MM-dd").parse(conf.get("end_date"));
					Date startlimit = new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01");  
					Date endlimit   = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-08"); 
					Date date = new SimpleDateFormat("yyyy-MM-dd").parse(tok[0]);  
					//filter out dates beyond start and end, then, filter out dates before 1-jan and after 8-April
					if ((date.compareTo(startdate)*date.compareTo(enddate)) <= 0
						&& (date.compareTo(startlimit)*date.compareTo(endlimit)) <= 0 ){ 
						cases.set(Long.parseLong(tok[3]));
						country.set(tok[1]); //country field
						context.write(country, cases);
					}
				} catch (ParseException e) {
                e.printStackTrace();
				}
			}
		}
	}

	// input key(country), input value(cases), key(country), output value(cases)
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable totalcases = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			totalcases.set(sum);
			context.write(key, totalcases);
		}
	}
	
	public static void main(String[] args)  throws Exception {
		long start = new Date().getTime();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[3])))
			fs.delete(new Path(args[3]), true);
		
		//cover all error cases for the dates
		try{		
			//Dates should not be in invalid format. 
			Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
			Date enddate   = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
			
			//Start date should be before end date. 
			if(startdate.after(enddate)){
				System.out.println("Start Dates is after end date");
				System.exit(0);
			}

			//The dates should be inside the given data range only.
			Date startlimit = new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01");  
			Date endlimit   = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-08"); 
			if(((startdate.compareTo(startlimit)*startdate.compareTo(endlimit)) > 0)
				|| ((enddate.compareTo(startlimit)*enddate.compareTo(endlimit)) > 0)){
				System.out.println("Start Dates or/and end date are out of range");
				System.exit(0);
			}
		}catch (ParseException e) {
			System.out.println("Dates are invalid");
			System.exit(0);
		}

        conf.set("start_date", args[1]);
		conf.set("end_date", args[2]);

		Job myjob = Job.getInstance(conf, "Covid19_2, total covid cases for every country");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0])); // input
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3])); //output
		//System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		boolean status = myjob.waitForCompletion(true);        
		long end = new Date().getTime();
		System.out.println("Hadoop Task 2 took "+(end-start) + "milliseconds");
		System.exit(status?1:0);
	}
}
