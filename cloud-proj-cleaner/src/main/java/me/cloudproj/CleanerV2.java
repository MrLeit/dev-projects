package me.cloudproj;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CleanerV2 {

    public static void main(String[] args) {
        String pathToData = args[0];
        String pathToProcessedData = args[1];

        //Create the output file
        try (final PrintWriter output = new PrintWriter(pathToProcessedData)) {
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
                                            String flightNo = record.get("FlightNum");
                                            String uniqueCarrier = record.get("UniqueCarrier");
                                            String origin = record.get("Origin");
                                            String dest = record.get("Dest");
                                            String arrDelay = record.get("ArrDelay");

                                            output.println(date + "," +
                                                    depTime + "," +
                                                    uniqueCarrier + "," +
                                                    flightNo + "," +
                                                    origin + "," +
                                                    dest + "," +
                                                    arrDelay);
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

}
