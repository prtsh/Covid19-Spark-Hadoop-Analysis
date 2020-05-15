
import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.lang.Long;
import java.text.ParseException;

//import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;

/*
The first task is to count the total number of reported cases for every country/location till April 8th, 
2020 
*/

public class Covid19_1 {
	// input key(object), input value(record), output key(country), output value(covid cases)
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
	private LongWritable cases = new LongWritable(0);
    private Text country = new Text();
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String flag = conf.get("flag");
            String[] tok = value.toString().split(",");
            //System.out.println(flag);//debug 
            //System.out.println(tok[1]); //debug
            try {
                Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01");  
                Date enddate   = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-08"); 
                Date date = new SimpleDateFormat("yyyy-MM-dd").parse(tok[0]);  
                if((date.compareTo(startdate)*date.compareTo(enddate)) <= 0
                    && tok.length == 4 && tok[0] != "date"){//filter out out-of-range dates, malformed fields
                    if(flag == "true"){
                        cases.set(Long.parseLong(tok[2]));
                        country.set(tok[1]); //country field
                        context.write(country, cases);
                    }
                    else{
                        if(!tok[1].equals("World") && !tok[1].equals("International")){
                            //System.out.println(tok[1]);
                            cases.set(Long.parseLong(tok[2]));
                            country.set(tok[1]); //country field
                            context.write(country, cases);
                        }
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
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
        Configuration conf = new Configuration();
        conf.set("flag", args[1]);
		Job myjob = Job.getInstance(conf, "Covid19_1, total covid cases for every country");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
        myjob.setOutputValueClass(LongWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[2])))
            fs.delete(new Path(args[2]), true);

		FileInputFormat.addInputPath(myjob, new Path(args[0])); // input
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2])); //output
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
