package me.cloudproj.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

public class CassandraWriter {

    public final static String tomTravelQuery =
            "insert into mapred.tom_travel (ampm, flightDate, flightTime, carrier, flightNo, origin, dest, delay) values (?, ?, ?, ?, ?, ?, ?, ?)";

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
        this.preparedStatement = this.session.prepare(tomTravelQuery);
    }

    public void writeTomTravelEntry(
            String ampm,
            String flightDate,
            String flightTime,
            String carrier,
            String flightNo,
            String origin,
            String dest,
            double delay) {
        Session session = this.getSession();

        try {
            session.execute(this.preparedStatement.bind(ampm, flightDate, flightTime, carrier, flightNo, origin, dest, delay));
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