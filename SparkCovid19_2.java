/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.lang.Long;
import java.text.ParseException;
import java.lang.Double;
import java.io.*;

/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

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

public class SparkCovid19_2 {
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Absolute path to the cache file
     * args[3]: Output file path on distributed file system
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        long start = new Date().getTime();

        //essential to run any spark code 
        SparkConf conf = new SparkConf().setAppName("SparkCovid19_2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //delete the output hdfs folder
        Configuration confhadoop = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(confhadoop);
        if (fs.exists(new Path(args[2]))){
                fs.delete(new Path(args[2]), true);
        }
        String output = args[2];

        // fill up the population map from the populations.csv
        HashMap<String, Double> country_pop_map = new HashMap<>(); //country, population
        
        String line = ""; 
		Path getFilePath = new Path(args[1].toString()); 
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath))); 
		while ((line = reader.readLine()) != null) { 
			if (!line.toString().contains("countriesAndTerritories")){ //ignore header
				String[] words = line.split(","); 
				//System.out.println(line); //debug
				if(words.length == 5 && words[4] != null){ //location and population
                    country_pop_map.put(words[1], Double.parseDouble(words[4]));
                    //System.out.println(country_pop_map.get(words[1])); //debug
                }
			}
        } 
        
        //declare broadcast variable from the hashmap
        Broadcast<HashMap<String, Double>> broadcastVar = sc.broadcast(country_pop_map);

        System.out.println("Spark Covid19_2, cases per million");
        try{
            /* load input data to RDD */
            JavaRDD<String> dataRDD = sc.textFile(args[0]);

            JavaPairRDD<String, Double> counts =
                dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
                    public Iterator<Tuple2<String, Double>> call(String value){
                    String[] words = value.split(",");
                    List<Tuple2<String, Double>> retWords =
                        new ArrayList<Tuple2<String, Double>>();
                    //parse line: date-country-cases-deaths
                    if(!value.toString().contains("date")){
                        Double cases = Double.parseDouble(words[2]);
                        Double denom = broadcastVar.value().get(words[1]);
                        //System.out.println(words[1]); //debug
                        //System.out.println(denom); //debug
                        if(denom != null){
                            cases = (cases*1e6)/denom;
                            retWords.add(new Tuple2<String, Double>(words[1], cases));
                        }
                    }
                    return retWords.iterator();
                    }
                }).reduceByKey(new Function2<Double, Double, Double>(){
                    public Double call(Double x, Double y){
                        return (x+y);
                    }
                });
        
            counts.saveAsTextFile(output);
        }catch (Exception e) {
            e.printStackTrace();
        }
        long end = new Date().getTime();
        System.out.println("Spark Task 2 took "+(end-start) + "milliseconds");
    }
}
