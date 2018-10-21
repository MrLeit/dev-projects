package me.cloudproj.solutions;

import kafka.api.OffsetRequest;
import me.cloudproj.util.AirportCarrierDepartureDelayCassandraHelper;
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
public class AirportCarrierDepartureDelay {

    private static final class SplitterBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("airportCarrier", "depDelay"));
        }

        @Override
        public void execute(Tuple input) {
            String value = input.getString(0);

            String[] tokens = value.split(",", -1);
            if (!tokens[3].isEmpty() && !tokens[4].isEmpty() && !tokens[6].isEmpty()) {
                collector.emit(new Values(tokens[4] + "," + tokens[3], Double.valueOf(tokens[6])));
            }

            collector.ack(input);
        }

    }

    private static final class AggregatorBolt extends BaseRichBolt {

        private AirportCarrierDepartureDelayCassandraHelper ccHelper;

        private static final Long timeBetweenBatches = TimeUnit.SECONDS.toMillis(10);
        private ScheduledExecutorService batchPrinter;

        private Map<String, Double> previousDelay;
        private Map<String, RunningAverager> airportCarrierAveragers;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;

            this.previousDelay = new HashMap<>();
            this.airportCarrierAveragers = new ConcurrentHashMap<>();

            this.ccHelper = new AirportCarrierDepartureDelayCassandraHelper();
            this.ccHelper.createConnection();

            this.batchPrinter = Executors.newSingleThreadScheduledExecutor();
            this.batchPrinter.scheduleAtFixedRate(() ->
                    printCurrentResultsPeriodically(), timeBetweenBatches, timeBetweenBatches, TimeUnit.MILLISECONDS);
        }

        private void printCurrentResultsPeriodically() {
            try {
                try (PrintWriter output = new PrintWriter("/tmp/airportCarrierDelays.txt")) {
                    for (Map.Entry<String, RunningAverager> entry : airportCarrierAveragers.entrySet()) {
                        String[] tokens = entry.getKey().split(",");
                        String airport = tokens[0];
                        String carrier = tokens[1];
                        double delay = entry.getValue().getCurrentAverage();
                        double prevDelay = previousDelay.get(airport + carrier) != null
                                ? previousDelay.get(airport + carrier)
                                : 0.0;

                        ccHelper.writeAirportAirlineEntry(
                                airport,
                                carrier,
                                prevDelay,
                                delay);

                        previousDelay.put(airport + carrier, delay);

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
            String airportCarrier = input.getStringByField("airportCarrier");
            Double depDelay = input.getDoubleByField("depDelay");

            if (!airportCarrierAveragers.containsKey(airportCarrier)) {
                airportCarrierAveragers.put(airportCarrier, RunningAverager.newAverager());
            }
            airportCarrierAveragers.get(airportCarrier).newAverage(depDelay);

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
        builder.setBolt("splitter-bolt", new AirportCarrierDepartureDelay.SplitterBolt(), 20)
                .shuffleGrouping("kafka-spout");
        builder.setBolt("aggregator-bolt", new AirportCarrierDepartureDelay.AggregatorBolt(), 1)
                .globalGrouping("splitter-bolt");

        StormSubmitter.submitTopology("AirportCarrierDepartureDelay", config, builder.createTopology());
    }

}
