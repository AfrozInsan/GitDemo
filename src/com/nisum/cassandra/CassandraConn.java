package com.nisum.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CassandraConn {
	
	private Cluster cluster;
	private Session session;
	
	public boolean connect(String address, int port){
		this.cluster = Cluster.builder().addContactPoint(address).withPort(port).build();
		
		Metadata metadata = cluster.getMetadata();
		
		for(Host host:metadata.getAllHosts()){
			System.out.println("DataCenter::"+ host.getDatacenter()+" Host::"+host.getAddress()+"RacK::"+host.getRack());
		}
		this.session = cluster.connect();
		if(this.session != null){
			return true;
		}
		
		return false;
	}
	
	public Session getSession(){
		return this.session;
	}
	
	public void close(){
		this.cluster.close();
	}

}
