package me.cloudproj.task2;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CleanerV3 {

    public static void main(String[] args) {
        String pathToData = args[0];
        String topic = args[1];

        //Create the output file
        try (KafkaProducer<String, String> myProducer = createProducer()) {
            // Iterate all files
            System.out.println("Will process the following dir set");
            Files.list(Paths.get(pathToData)).forEach(System.out::println);

            Files.list(Paths.get(pathToData)).forEach(path -> {
                if(path.endsWith("2008")) {
                    try {
                        System.out.println("Will process the following zip set");
                        Files.list(path).forEach(System.out::println);

                        Files.list(path).forEach(zip -> {
                            System.out.println("Processing zip file " + zip.toFile().getAbsolutePath());

                            try (ZipFile zipFile = new ZipFile(zip.toFile().getAbsolutePath())) {
                                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                                while (entries.hasMoreElements()) {
                                    final ZipEntry entry = entries.nextElement();
                                    if (entry.getName().endsWith(".csv")) {
                                        System.out.println("Processing zip entry " + entry.getName());

                                        CSVParser parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new InputStreamReader(zipFile.getInputStream(entry)));

                                        for (CSVRecord record : parser) {
                                            String date = record.get("FlightDate");
                                            String depTime = record.get("CRSDepTime");
                                            String uniqueCarrier = record.get("UniqueCarrier");
                                            String flightNo = record.get("FlightNum");
                                            String origin = record.get("Origin");
                                            String dest = record.get("Dest");
                                            String arrDelay = record.get("ArrDelay");

                                            myProducer.send(new ProducerRecord<>(
                                                    topic,
                                                    String.format("%s,%s,%s,%s,%s,%s,%s", date, depTime, uniqueCarrier, flightNo, origin, dest, arrDelay)));
                                        }
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", "0");
        properties.put("batch.size", "16384");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("linger.ms", "0");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("block.on.buffer.full", "true");

        return new KafkaProducer<>(properties);
    }

}
