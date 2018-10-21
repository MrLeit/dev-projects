package me.cloudproj.solutions;

import kafka.api.OffsetRequest;
import me.cloudproj.util.RunningAverager;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.time.DayOfWeek;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Created by mrleit on 4/06/17.
 */
public class DayOfWeekArrivalDelay {

    private static final class SplitterBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("dow", "arrDelay"));
        }

        @Override
        public void execute(Tuple input) {
            String value = input.getString(0);

            String[] tokens = value.split(",", -1);
            if (!tokens[1].isEmpty() && !tokens[7].isEmpty()) {
                collector.emit(new Values(Integer.valueOf(tokens[1]), Double.valueOf(tokens[7])));
            }

            collector.ack(input);
        }

    }

    private static final class AggregatorBolt extends BaseRichBolt {

        private static final Long timeBetweenBatches = TimeUnit.SECONDS.toMillis(10);
        private ScheduledExecutorService batchPrinter;

        private Map<Integer, RunningAverager> dayOfWeekAveragers;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;

            this.dayOfWeekAveragers = new ConcurrentHashMap<>();
            IntStream.range(1, 8).forEach(i -> dayOfWeekAveragers.put(i, RunningAverager.newAverager()));

            this.batchPrinter = Executors.newSingleThreadScheduledExecutor();
            this.batchPrinter.scheduleAtFixedRate(() ->
                    printCurrentResultsPeriodically(), timeBetweenBatches, timeBetweenBatches, TimeUnit.MILLISECONDS);
        }

        private void printCurrentResultsPeriodically() {
            try {
                try (PrintWriter output = new PrintWriter("/tmp/dayDelays.txt")) {
                    for (Map.Entry<Integer, RunningAverager> entry : dayOfWeekAveragers.entrySet()) {
                        output.println(DayOfWeek.of(entry.getKey()).toString() + " " + entry.getValue().toString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // No output
        }

        @Override
        public void execute(Tuple input) {
            Integer dow = input.getIntegerByField("dow");
            Double arrDelay = input.getDoubleByField("arrDelay");

            dayOfWeekAveragers.get(dow).newAverage(arrDelay);

            collector.ack(input);
        }

    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setNumWorkers(10);
        config.setMaxSpoutPending(1000);
        config.setMessageTimeoutSecs(60);
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx8G");

        String zkConnString = "localhost:2181";
        String topic = "aviation";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.startOffsetTime = OffsetRequest.EarliestTime();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig), 4);
        builder.setBolt("splitter-bolt", new DayOfWeekArrivalDelay.SplitterBolt(), 20)
                .shuffleGrouping("kafka-spout");
        builder.setBolt("aggregator-bolt", new DayOfWeekArrivalDelay.AggregatorBolt(), 1)
                .globalGrouping("splitter-bolt");

        StormSubmitter.submitTopology("DayOfWeekArrivalDelay", config, builder.createTopology());
    }

}
