package me.cloudproj.solutions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by mrleit on 20/05/17.
 */
public class AverageArrivalDelayWeekday {

    private static long corruptRecords = 0;

    public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private IntWritable dayOfWeek = new IntWritable();
        private DoubleWritable arrDelay = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",", -1);

            if(tokens[1].isEmpty() || tokens[8].isEmpty()) {
                corruptRecords++;
            } else {
                dayOfWeek.set(Integer.valueOf(tokens[1]));
                arrDelay.set(Double.parseDouble(tokens[8]));
                context.write(dayOfWeek, arrDelay);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, DoubleWritable, Text, DoubleWritable> {

        private static final String[] dayStrings = { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" };

        private Text dayOfWeek = new Text();
        private DoubleWritable arrDelay = new DoubleWritable();

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)  throws IOException, InterruptedException {
            double sum = 0.0;
            double count = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            dayOfWeek.set(convertToWeekDay(key.get()));
            arrDelay.set(Math.round((sum / count) * 100.0) / 100.0);
            context.write(dayOfWeek, arrDelay);
        }

        private String convertToWeekDay(Integer dayOfWeek) {
            return dayStrings[dayOfWeek - 1];
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "average arrival delay");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(AverageArrivalDelayWeekday.Map.class);
        job.setReducerClass(AverageArrivalDelayWeekday.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println("Count of corrupt records: " + corruptRecords);
    }

}
