package com.stockintel.mapred;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class WordListCAS {
    static final String CASSANDRA_HOST = "localhost";
    static final String KEYSPACE = "text_ks";
    static final String COLUMN_FAMILY = "text_table";
    static final String COLUMN_NAME = "text_col";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: WordListCAS <Casandra host>");
            System.exit(-1);
        }
        
        Cluster cluster = Cluster.builder()
                .addContactPoints(args[0])
                .build();
        Session session = cluster.connect(KEYSPACE);
        
        String cqlStatement2 = String.format("select %s from %s;",
                COLUMN_NAME,
                COLUMN_FAMILY);
        ResultSet result = session.execute(cqlStatement2);

        for (Row row : result.all()) {
            System.out.println(row.toString());
        }
        System.exit(0);
    }

}
