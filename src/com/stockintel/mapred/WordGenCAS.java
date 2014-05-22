package com.stockintel.mapred;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class WordGenCAS {
    static final String CASSANDRA_HOST = "localhost";
    static final String KEYSPACE = "text_ks";
    static final String COLUMN_FAMILY = "text_table";
    static final String COLUMN_NAME = "text_col";

    public static void main(String[] args) throws Exception {
        Cluster cluster = Cluster.builder()
                .addContactPoints(CASSANDRA_HOST)
                .build();
        Session session = cluster.connect(KEYSPACE);

        if (args.length < 1) {
            System.out.println("Usage: Wordgen <input file>");
            System.exit(-1);
        }

        InputStream fis = new FileInputStream(args[0]);
        InputStreamReader in = new InputStreamReader(fis, "UTF-8");
        BufferedReader br = new BufferedReader(in);
        
        String line;
        int lineCount = 0;
        while ( (line = br.readLine()) != null) {
            line = line.replaceAll("'", " ");
            line = line.trim();
            if (line.isEmpty())
            	continue;
            System.out.println("[" + line + "]");
            String cqlStatement2 = String.format("insert into %s (%s) values ('%s');",
                    COLUMN_FAMILY,
                    COLUMN_NAME,
                    line);
            ResultSet result = session.execute(cqlStatement2);
            lineCount++;
        }

        System.out.println("Total lines written: " + lineCount);
        System.exit(0);
    }

}
