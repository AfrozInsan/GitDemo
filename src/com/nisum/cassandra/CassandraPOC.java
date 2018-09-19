package com.nisum.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraPOC {

	public static void main(String[] args) {
	System.out.println("Cassandra POC to esstablish the connection.");
	CassandraConn connCass = new CassandraConn();
	String ipAddress = args.length > 0 ? args[0]:"localhost";
	int port = args.length >1 ? Integer.parseInt(args[1]): 9042;
	
	if(connCass.connect(ipAddress, port)){
		System.out.println("Connection Esstablished");
		Session session = connCass.getSession();
		/*final String createMovieCql =
			     "CREATE TABLE admin_v1.movies (title varchar, year int, description varchar, "
			   + "mmpa_rating varchar, dustin_rating varchar, PRIMARY KEY (title, year))";
		
			session.execute(createMovieCql);*/
		
		/*final String insertCql = "INSERT INTO admin_v1.movies (title, year, description, mmpa_rating, dustin_rating)"
				+ " VALUES ('title', 2018, 'description', 'mmpaRating', 'dustinRating');";
		session.execute(insertCql);*/
		
		/*String selectCql = "select * from admin_v1.movies where title = 'title'";
		ResultSet rs = session.execute(selectCql);
		Row row = rs.one();
		System.out.println(row.getString(0)+" "+row.getInt(1)+" "+row.getString(2)+" "+row.getString(3)+" "+row.getString(4));*/
		
		String delectCql = "drop table admin_v1.movies";
		session.execute(delectCql);
		System.out.println("table Created.");
		connCass.close();
	}
	
		
		
		
	}

}
