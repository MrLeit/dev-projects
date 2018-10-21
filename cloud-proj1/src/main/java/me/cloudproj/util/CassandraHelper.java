package me.cloudproj.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

public class CassandraHelper {

    public final static String airportAirlineDepartQuery =
            "INSERT INTO mapred.airport_airline_departure_delay (airport, airline, delay) VALUES(?, ?, ?)";
    public final static String airportAirportDepartQuery =
            "INSERT INTO mapred.airport_airport_departure_delay (origin, dest, delay) VALUES(?, ?, ?)";
    public final static String airportAirportAirlineArrivalQuery =
            "INSERT INTO mapred.airport_airport_airline_arrival_delay (origin, dest, airline, delay) VALUES (?, ?, ?, ?)";
    public final static String airportAirportArrivalQuery =
            "INSERT INTO mapred.airport_airport_arrival_delay (origin, dest, delay) VALUES (?, ?, ?)";

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

    public void createConnection(String node, String query) {
        this.cluster = Cluster.builder().addContactPoint(node).build();

        Metadata metadata = cluster.getMetadata();

        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();

        this.prepareQueries(query);

    }

    public void closeConnection() {
        cluster.close();
    }

    private void prepareQueries(String query) {
        System.out.println("Starting prepareQueries()");
        this.preparedStatement = this.session.prepare(query);
    }

    public void writeAirportAirlineEntry(String airport, String airline, double depDelay) {
        Session session = this.getSession();

        try {
            session.execute(this.preparedStatement.bind(airport, airline, depDelay));
            //session.executeAsync(this.preparedStatement.bind(key));
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

    public void writeAirportAirportEntry(String origin, String dest, double depDelay) {
        Session session = this.getSession();

        try {
            session.execute(this.preparedStatement.bind(origin, dest, depDelay));
            //session.executeAsync(this.preparedStatement.bind(key));
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

    public void writeAirportAirportAirlineEntry(String origin, String dest, String airline, double depDelay) {
        Session session = this.getSession();

        try {
            session.execute(this.preparedStatement.bind(origin, dest, airline, depDelay));
            //session.executeAsync(this.preparedStatement.bind(key));
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