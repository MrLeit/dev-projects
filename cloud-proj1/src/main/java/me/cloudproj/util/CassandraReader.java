package me.cloudproj.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

/**
 * Created by mrleit on 25/05/17.
 */
public class CassandraReader {

    public final static String flightLegQuery =
            "select * from mapred.tom_travel where origin = ? and dest = ? and flightDate = ? and ampm = ? allow filtering";

    private Cluster cluster;
    private Session session;

    private PreparedStatement preparedStatement;

    public Session getSession() {
        if (this.session == null && (this.cluster == null || this.cluster.isClosed())) {
            System.out.println("Cluster not started or closed");
        } else if (this.session.isClosed()) {
            System.out.println("session is closed. Creating a session");
            this.session = this.cluster.connect();
        }

        return this.session;
    }

    public void createConnection(String node) {
        this.cluster = Cluster.builder().addContactPoint(node).build();

        Metadata metadata = cluster.getMetadata();

        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();

        this.prepareQueries();

    }

    public void closeConnection() {
        cluster.close();
    }

    private void prepareQueries() {
        System.out.println("Starting prepareQueries()");
        this.preparedStatement = this.session.prepare(flightLegQuery);
    }

    public void findFlightRecord(
            String start,
            String stopOver,
            String end,
            String firstFlightDate,
            String secondFlightDate) {
        Session session = this.getSession();

        try {
            ResultSet firstLegResult = session.execute(this.preparedStatement.bind(start, stopOver, firstFlightDate, "B"));
            ResultSet secondLegResult = session.execute(this.preparedStatement.bind(stopOver, end, secondFlightDate, "A"));

            if(firstLegResult.isExhausted() || secondLegResult.isExhausted()) {
                System.out.println("No results");
            } else {
                Row firstFlightRow = firstLegResult.one();
                Row secondFlightRow = secondLegResult.one();

                double totalDelay = firstFlightRow.get("delay", Double.class) +
                        secondFlightRow.get("delay", Double.class);

                System.out.println(start + "->" + stopOver + "->" + end + "," + firstFlightDate);
                System.out.println("Total arrival delay: " + totalDelay);
                System.out.println("First leg:");
                System.out.println("  Origin: " + start);
                System.out.println("  Destination: " + stopOver);
                System.out.println("  Airline/Flight No: " + firstFlightRow.get("carrier", String.class) + " " + firstFlightRow.get("flightNo", String.class));
                System.out.println("  Sched Depart: " + firstFlightRow.get("flightTime", String.class) + " " + firstFlightDate);
                System.out.println("  Arrival Delay: " + firstFlightRow.get("delay", Double.class));
                System.out.println("Second leg:");
                System.out.println("  Origin: " + stopOver);
                System.out.println("  Destination: " + end);
                System.out.println("  Airline/Flight No: " + secondFlightRow.get("carrier", String.class) + " " + secondFlightRow.get("flightNo", String.class));
                System.out.println("  Sched Depart: " + secondFlightRow.get("flightTime", String.class) + " " + secondFlightRow);
                System.out.println("  Arrival Delay: " + secondFlightRow.get("delay", Double.class));
            }
        } catch (NoHostAvailableException e) {
            System.out.printf("No host in the %s cluster can be contacted to execute the query.\n",
                    session.getCluster());
            Session.State st = session.getState();
            for (Host host : st.getConnectedHosts()) {
                System.out.println("In flight queries::" + st.getInFlightQueries(host));
                System.out.println("open connections::" + st.getOpenConnections(host));
            }

        } catch (QueryExecutionException e) {
            System.out.println("An exception was thrown by Cassandra because it cannot " +
                    "successfully execute the query with the specified consistency level.");
        } catch (IllegalStateException e) {
            System.out.println("The BoundStatement is not ready.");
        }
    }

}
