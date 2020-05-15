import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.lang.Long;
import java.lang.Double;
import java.text.ParseException;
import java.util.Date;
import java.net.URI; 
import java.io.*;

//import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;

/*
Your third task is to output the total number of cases per 1 million population for every country
*/

public class Covid19_3 {
	// input key(object), input value(record), output key(country), output value(covid cases)
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private DoubleWritable cases = new DoubleWritable(0);
    private Text country   = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tok = value.toString().split(",");
			Configuration conf = context.getConfiguration();
			try{
                cases.set(Double.parseDouble(tok[2])); //cases
                country.set(tok[1]); //country
                context.write(country, cases);
			} catch (Exception e) {
                e.printStackTrace();
            }
		}
	}

	// input key(country), input value(cases), key(country), output value(cases)
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable casepermillion = new DoubleWritable();
		private HashMap<String, Double> country_pop_map = null;

		public void setup(Context context) throws IOException,  InterruptedException { 
			country_pop_map = new HashMap<>(); 
			URI[] cacheFiles = context.getCacheFiles(); 
			if (cacheFiles != null && cacheFiles.length > 0)  { 
				try { 
					String line = ""; 
					FileSystem fs = FileSystem.get(context.getConfiguration()); 
					Path getFilePath = new Path(cacheFiles[0].toString()); 
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath))); 
					while ((line = reader.readLine()) != null) { 
						if (!line.toString().contains("countriesAndTerritories")){
							String[] words = line.split(","); 
							//System.out.println(line); //debug
							if(words.length == 5) //location and population
								country_pop_map.put(words[1], Double.parseDouble(words[4]));
						}
					} 
				} 
				catch (Exception e) { 
					System.out.println("Unable to read the File"); 
					e.printStackTrace();
				} 
			} 
		} 
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			Double sum = 0.0;
			try { 
				for (DoubleWritable tmp: values) {
					sum += tmp.get();
				}
				double population = country_pop_map.get(key.toString());
				//System.out.println(population); //debug
				casepermillion.set((sum/population)*1e6);
				context.write(key, casepermillion);
			}
			catch (Exception e) { 
				//e.printStackTrace();
			} 
		}
	}
	
	public static void main(String[] args)  throws Exception {
		long start = new Date().getTime();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[2])))
            fs.delete(new Path(args[2]), true);

		Job myjob = Job.getInstance(conf, "Covid19_3, total covid cases for every country");
		String cachepath = new String (args[1]); //cache file
		try { 
            myjob.addCacheFile(new URI(cachepath)); 
        } 
        catch (Exception e) { 
			e.printStackTrace();
		} 
		
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0])); // input
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2])); //output

		boolean status = myjob.waitForCompletion(true);        
		long end = new Date().getTime();
		System.out.println("Hadoop Task 3 took "+(end-start) + "milliseconds");
		System.exit(status?1:0);
	}
}
