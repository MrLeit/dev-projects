package me.cloudproj.solutions.tom;

import me.cloudproj.util.CassandraWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * Created by mrleit on 22/05/17.
 */
public class Tom {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text uniqueOriginDest = new Text();
        private Text details = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",", -1);

            if (tokens[0].isEmpty() || tokens[1].isEmpty() || tokens[2].isEmpty() || tokens[3].isEmpty() ||
                    tokens[4].isEmpty() || tokens[5].isEmpty() || tokens[6].isEmpty()) {
                return;
            }
            int time = Integer.valueOf(tokens[2]);
            if (time <= 1200) {
                uniqueOriginDest.set("B," + tokens[0] + tokens[4] + tokens[5]);
            } else if (time >= 1200) {
                uniqueOriginDest.set("A," + tokens[0] + tokens[4] + tokens[5]);
            }
            details.set(line);
            context.write(uniqueOriginDest, details);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private static final CassandraWriter ccWriter = new CassandraWriter();

        private static DoubleWritable shortest = new DoubleWritable();
        private static Text shortestDelayFlight = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double shortestDelay = Double.MAX_VALUE;
            for (Text val : values) {
                String line = val.toString();
                String[] tokens = line.split(",", -1);
                double delay = Double.valueOf(tokens[6]);
                if (delay < shortestDelay) {
                    shortestDelay = delay;
                    shortestDelayFlight.set(val);
                }
            }
            if (shortestDelayFlight != null) {
                shortest.set(shortestDelay);
                context.write(key, shortestDelayFlight);

                String[] tokens = shortestDelayFlight.toString().split(",");
                ccWriter.writeTomTravelEntry(
                        key.toString().split(",")[0],
                        tokens[0],
                        tokens[1],
                        tokens[2],
                        tokens[3],
                        tokens[4],
                        tokens[5],
                        shortestDelay);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            ccWriter.createConnection("");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ccWriter.closeConnection();
        }

    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "tom");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Tom.Map.class);
        job.setReducerClass(Tom.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

