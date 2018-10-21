package me.cloudproj.solutions;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by mrleit on 25/05/17.
 */
public class TomFlightFinder {

    public static final void main(String args[]) throws Exception {
        CassandraReader reader = new CassandraReader();
        reader.createConnection("");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String secondDate = format.format(new Date(format.parse(args[3]).getTime() + TimeUnit.DAYS.toMillis(2)));

        reader.findFlightRecord(args[0], args[1], args[2], args[3], secondDate);

        System.exit(1);
    }

}
