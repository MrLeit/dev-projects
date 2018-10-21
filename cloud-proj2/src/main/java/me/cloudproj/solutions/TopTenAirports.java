package me.cloudproj.solutions;

import kafka.api.OffsetRequest;
import me.cloudproj.util.ValueComparator;
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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by mrleit on 3/06/17.
 */
public class TopTenAirports {

    private static final class SplitterBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("airport"));
        }

        @Override
        public void execute(Tuple input) {
            String value = input.getString(0);

            String[] tokens = value.split(",", -1);
            if (!tokens[0].isEmpty()) {
                collector.emit(new Values(tokens[0]));
            }
            if (!tokens[1].isEmpty()) {
                collector.emit(new Values(tokens[1]));
            }

            collector.ack(input);
        }

    }

    private static final class AggregatorBolt extends BaseRichBolt {

        private static final Long timeBetweenBatches = TimeUnit.SECONDS.toMillis(10);
        private ScheduledExecutorService batchPrinter;

        Map<String, Integer> airportCounts;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.airportCounts = new HashMap<>();
            this.batchPrinter = Executors.newSingleThreadScheduledExecutor();
            this.batchPrinter.scheduleAtFixedRate(() ->
                printCurrentResultsPeriodically(), timeBetweenBatches, timeBetweenBatches, TimeUnit.MILLISECONDS);
        }

        private void printCurrentResultsPeriodically() {
            if (!airportCounts.isEmpty()) {
                try {
                    Comparator<String> comparator = new ValueComparator(airportCounts);
                    Map<String, Integer> result = new TreeMap<>(comparator);
                    result.putAll(airportCounts);

                    try (PrintWriter output = new PrintWriter("/tmp/topTen.txt")) {
                        int outputted = 0;
                        for (Map.Entry<String, Integer> entry : result.entrySet()) {
                            output.println(entry.getKey() + " " + entry.getValue());
                            if (++outputted > 9) {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // No output
        }

        @Override
        public void execute(Tuple input) {
            String value = input.getString(0);

            if (airportCounts.containsKey(value)) {
                airportCounts.put(value, airportCounts.get(value) + 1);
            } else {
                airportCounts.put(value, 1);
            }

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
        String topic = "lite";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.startOffsetTime = OffsetRequest.EarliestTime();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig), 4);
        builder.setBolt("splitter-bolt", new SplitterBolt(), 20)
                .shuffleGrouping("kafka-spout");
        builder.setBolt("aggregator-bolt", new AggregatorBolt(), 1)
                .globalGrouping("splitter-bolt");

        StormSubmitter.submitTopology("TopTenAirports", config, builder.createTopology());
    }

}