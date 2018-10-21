package me.cloudproj.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

/**
 * Created by mrleit on 4/06/17.
 */
public class OriginDestinationCarrierArrivalDelayCassandraHelper {

    private final static String origDestCarrierArriveDeleteQuery =
            "DELETE FROM storm.orig_dest_carrier_arrival_delay where orig = ? and dest = ? and carrier = ? and delay = ?";
    private final static String origDestCarrierArriveInsertQuery =
            "INSERT INTO storm.orig_dest_carrier_arrival_delay (orig, dest, carrier, delay) VALUES(?, ?, ?, ?)";

    private Cluster cluster;
    private Session session;

    private PreparedStatement deleteStatement;
    private PreparedStatement insertStatement;

    private Session getSession() {
        if (this.session == null && (this.cluster == null || this.cluster.isClosed())) {
            System.out.println("Cluster not started or closed");
        } else if (this.session.isClosed()) {
            System.out.println("session is closed. Creating a session");
            this.session = this.cluster.connect();
        }

        return this.session;
    }

    public void createConnection() {
        this.cluster = Cluster.builder().addContactPoint("").build();

        Metadata metadata = cluster.getMetadata();

        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();

        this.prepareQueries();
    }

    private void prepareQueries() {
        System.out.println("Starting prepareQueries()");
        this.deleteStatement = this.session.prepare(origDestCarrierArriveDeleteQuery);
        this.insertStatement = this.session.prepare(origDestCarrierArriveInsertQuery);
    }

    public void writeOrigDestCarrierEntry(String orig, String dest, String carrier, double prevDelay, double newDelay) {
        Session session = this.getSession();

        try {
            session.execute(this.deleteStatement.bind(orig, dest, carrier, prevDelay));
            session.execute(this.insertStatement.bind(orig, dest, carrier, newDelay));
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
