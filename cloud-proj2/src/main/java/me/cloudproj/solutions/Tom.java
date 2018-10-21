package me.cloudproj.solutions;

import kafka.api.OffsetRequest;
import me.cloudproj.util.TomCassandraHelper;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by mrleit on 4/06/17.
 */
public class Tom {

    private static final class SplitterBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "flightDetails"));
        }

        @Override
        public void execute(Tuple input) {
            String value = input.getString(0);

            String[] tokens = value.split(",", -1);
            if (tokens[0].isEmpty() || tokens[1].isEmpty() || tokens[2].isEmpty() || tokens[3].isEmpty() ||
                    tokens[4].isEmpty() || tokens[5].isEmpty() || tokens[6].isEmpty()) {
                collector.ack(input);
                return;
            }
            String key = "";
            int time = Integer.valueOf(tokens[1]);
            if (time <= 1200) {
                key = "B," + tokens[0] + tokens[4] + tokens[5];
            } else if (time >= 1200) {
                key = "A," + tokens[0] + tokens[4] + tokens[5];
            }

            collector.emit(new Values(key, value));

            collector.ack(input);
        }

    }

    private static final class AggregatorBolt extends BaseRichBolt {

        private TomCassandraHelper ccHelper;

        private static final Long timeBetweenBatches = TimeUnit.SECONDS.toMillis(10);
        private ScheduledExecutorService batchPrinter;

        private Map<String, Double> previousDelay;
        private Map<String, String> bestFlightsForTom;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;

            this.previousDelay = new HashMap<>();
            this.bestFlightsForTom = new ConcurrentHashMap<>();

            this.ccHelper = new TomCassandraHelper();
            this.ccHelper.createConnection();

            this.batchPrinter = Executors.newSingleThreadScheduledExecutor();
            this.batchPrinter.scheduleAtFixedRate(() ->
                    printCurrentResultsPeriodically(), timeBetweenBatches, timeBetweenBatches, TimeUnit.MILLISECONDS);
        }

        private void printCurrentResultsPeriodically() {
            try {
                try (PrintWriter output = new PrintWriter("/tmp/tom.txt")) {
                    for (Map.Entry<String, String> entry : bestFlightsForTom.entrySet()) {
                        String[] keyTokens = entry.getKey().split(",");
                        String[] valueTokens = entry.getValue().split(",");

                        String ampm = keyTokens[0];
                        String flightDate = valueTokens[0];
                        String flightTime = valueTokens[1];
                        String carrier = valueTokens[2];
                        String flightNo = valueTokens[3];
                        String origin = valueTokens[4];
                        String dest = valueTokens[5];
                        Double arrDelay = Double.valueOf(valueTokens[6]);

                        double prevDelay = previousDelay.get(entry.getKey()) != null
                                ? previousDelay.get(entry.getKey())
                                : 0.0;

                        ccHelper.writeTomEntry(
                                ampm,
                                flightDate,
                                flightTime,
                                carrier,
                                flightNo,
                                origin,
                                dest,
                                prevDelay,
                                arrDelay);

                        previousDelay.put(entry.getKey(), arrDelay);

                        output.println(entry.getKey() + " " + entry.getValue().toString());
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
            String key = input.getStringByField("key");
            String flightDetails = input.getStringByField("flightDetails");

            if (!bestFlightsForTom.containsKey(key)) {
                bestFlightsForTom.put(key, flightDetails);
            } else {
                String bestFlight = bestFlightsForTom.get(key);

                String[] tokens = bestFlight.split(",");
                Double currentArrivalDelay = Double.valueOf(tokens[6]);

                tokens = flightDetails.split(",");
                Double newArrivalDelay = Double.valueOf(tokens[6]);

                if(newArrivalDelay < currentArrivalDelay) {
                    bestFlightsForTom.put(key, flightDetails);
                }
            }

            collector.ack(input);
        }

    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setNumWorkers(10);
        config.setMaxSpoutPending(1000);
        config.setMessageTimeoutSecs(60);
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx12G");

        String zkConnString = "localhost:2181";
        String topic = "tom";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.startOffsetTime = OffsetRequest.EarliestTime();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig), 4);
        builder.setBolt("splitter-bolt", new Tom.SplitterBolt(), 20)
                .shuffleGrouping("kafka-spout");
        builder.setBolt("aggregator-bolt", new Tom.AggregatorBolt(), 4)
                .fieldsGrouping("splitter-bolt", new Fields("key"));

        StormSubmitter.submitTopology("Tom", config, builder.createTopology());
    }

}
