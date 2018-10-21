package me.cloudproj.solutions;

import me.cloudproj.util.CassandraHelper;
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
 * Created by mrleit on 20/05/17.
 */
public class AirportAirportAirlineArrivalDelay {

    private static long corruptRecords = 0;

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text airportAirportArline = new Text();
        private DoubleWritable arrDelay = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",", -1);

            if(tokens[4].isEmpty() || tokens[5].isEmpty() || tokens[6].isEmpty() || tokens[8].isEmpty()) {
                corruptRecords++;
            } else {
                airportAirportArline.set(tokens[5] + "," + tokens[6] + "," + tokens[4]);
                arrDelay.set(Double.parseDouble(tokens[8]));
                context.write(airportAirportArline, arrDelay);
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private CassandraHelper cclient = new CassandraHelper();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)  throws IOException, InterruptedException {
            double sum = 0.0;
            double count = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            String origin = key.toString().split(",")[0];
            String dest = key.toString().split(",")[1];
            String airline = key.toString().split(",")[2];

            double arrDelay = Math.round((sum / count) * 100.0) / 100.0;

            // Hadoop out
            context.write(key, new DoubleWritable(arrDelay));

            // Cassandra
            cclient.writeAirportAirportAirlineEntry(origin, dest, airline, arrDelay);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            cclient.createConnection("", CassandraHelper.airportAirportAirlineArrivalQuery);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            cclient.closeConnection();
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "airport to airport by airline arrival delay");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(AirportAirportAirlineArrivalDelay.Map.class);
        job.setReducerClass(AirportAirportAirlineArrivalDelay.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println("Count of corrupt records: " + corruptRecords);
    }

}
