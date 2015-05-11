package org.jgimbel;
/**
 * Created by joel on 5/8/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DatesChecker {

    public static class DatesCheckerMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {
        DoubleWritable da;
        IntWritable ONE = new IntWritable(1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public DatesCheckerMapper(){}

        public Map< String, String> parseXML(String xml){
            Map < String, String > map = new HashMap < String, String > ();
            try {
                String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
                for (int i = 0; i < tokens.length - 1; i += 2) {
                    String key = tokens[i].trim();
                    String val = tokens[i + 1];
                    map.put(key.substring(0, key.length() - 1), val);
                }
            }
            catch (StringIndexOutOfBoundsException e) {
                System.err.println(xml);
            }

            return map;
        }

        public void map(Object key, Text value, Context c) throws IOException, InterruptedException{

            Map<String, String> v = parseXML(value.toString());

            if(v.containsKey("CreationDate")) {
                Calendar myCal = GregorianCalendar.getInstance();
                try {
                    Date d = sdf.parse(v.get("CreationDate"));
                    myCal.setTime(d);
                    double hour = ((double)myCal.get(Calendar.HOUR_OF_DAY)) / 24.0;

                    da = new DoubleWritable(myCal.get(Calendar.DAY_OF_WEEK) + hour);
                    c.write(da, ONE);
                }catch(ParseException e){
                    return; //Not worth dealing with
                }
            }


        }
    }

    public static class DatesCheckerCombiner extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
        public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable i : values){
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class DatesCheckerReducer extends Reducer<DoubleWritable, IntWritable, DoubleWritable, DoubleWritable>{
        public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable i : values){
                sum += i.get();
            }
            double avg = ((double)sum) / 21736594.0;
            DoubleWritable s = new DoubleWritable((avg*100));
            context.write(key, s);
        }
    }

    public static void main(String[] args) throws Exception{


        Configuration conf = new Configuration();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();

        if(args.length != 2){
            System.out.println("Your bad input is bad and you should feel bad");
            System.exit(1);
        }

        Job j = Job.getInstance(conf, "Date formating");

        j.setJarByClass(DatesChecker.class);
        j.setMapperClass(DatesCheckerMapper.class);

        j.setCombinerClass(DatesCheckerCombiner.class);
        j.setReducerClass(DatesCheckerReducer.class);

        j.setOutputKeyClass(DoubleWritable.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(j, new Path(otherArgs[1]));

        System.exit(j.waitForCompletion(true) ? 0 : 1);


    }
}
