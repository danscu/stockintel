package com.stockintel.app;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Main {

	public Main() {
	}

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder()
				.addContactPoints("192.168.1.4")
				.build();
		Session session = cluster.connect();
		String cqlStatement = "use stock;";
		session.execute(cqlStatement);

		String cqlStatement2 = "select * from dailyquote;";
		ResultSet result = session.execute(cqlStatement2);

		for (Row row : result.all()) {		
			System.out.println(row.toString());
		}
		System.exit(0);
	}

}
