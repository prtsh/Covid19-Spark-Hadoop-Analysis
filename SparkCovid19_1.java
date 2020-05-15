/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.lang.Long;
import java.text.ParseException;
import java.util.Date;

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

//import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

/*
Your second task is to modify your program to report total number of deaths for every location/country
in between a given range of dates.
*/

public class SparkCovid19_1 {
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Start date (YYYY-MM-DD)
     * args[2]: End date (YYYY-MM-DD)
     * args[3]: Output file path on distributed file system
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        long start = new Date().getTime();
        System.out.println("Spark Covid19_1, deaths in a range");
        try{
            /* essential to run any spark code */
            SparkConf conf = new SparkConf().setAppName("SparkCovid19_1");
            JavaSparkContext sc = new JavaSparkContext(conf);
            
            //delete the output hdfs folder
            Configuration confhadoop = new org.apache.hadoop.conf.Configuration();
            FileSystem fs = FileSystem.get(confhadoop);
            if (fs.exists(new Path(args[3])))
                fs.delete(new Path(args[3]), true);
            
            Date startdate = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
            Date enddate   = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
            Date startlimit = new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01");  
            Date endlimit   = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-08"); 
            String output = args[3];

            //cover all error cases for the dates
            //Start date should be before end date. 
            if(startdate.after(enddate)){
                System.out.println("Start Dates is after end date");
                System.exit(0);
            }

            //The dates should be inside the given data range only.
            if(((startdate.compareTo(startlimit)*startdate.compareTo(endlimit)) > 0)
                    || ((enddate.compareTo(startlimit)*enddate.compareTo(endlimit)) > 0)){
                System.out.println("Start Dates or/and end date are out of range");
                System.exit(0);
			}
        
            /* load input data to RDD */
            JavaRDD<String> dataRDD = sc.textFile(args[0]);

            JavaPairRDD<String, Integer> counts =
                dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
                    public Iterator<Tuple2<String, Integer>> call(String value){
                        String[] words = value.split(",");
                        
                        List<Tuple2<String, Integer>> retWords =
                            new ArrayList<Tuple2<String, Integer>>();
                        
                        //parse line: date-country-cases-deaths
                        if(!value.toString().contains("date")){
                            try{
                                Date date = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);  
                                //date should be between start and end, then date should be 
                                //between 1-Jan and 8-April
                                if ((date.compareTo(startdate)*date.compareTo(enddate)) <= 0
                                    && (date.compareTo(startlimit)*date.compareTo(endlimit)) <= 0 ){
                                    retWords.add(new Tuple2<String, Integer>(words[1], Integer.parseInt(words[3])));
                                }
                            }catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    return retWords.iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>(){
                    public Integer call(Integer x, Integer y){
                        return x+y;
                    }
                });
        
            counts.saveAsTextFile(output);
        }catch (Exception e) {
            System.out.println("Dates are not in correct format, or other issue, check stack trace below");
            e.printStackTrace();
        }
        
        long end = new Date().getTime();
        System.out.println("Spark Task 1 took "+(end-start) + "milliseconds");
    }
}
