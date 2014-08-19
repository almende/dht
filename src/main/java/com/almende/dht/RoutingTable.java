package com.almende.dht;

import java.util.logging.Logger;

public class RoutingTable {
    private static final Logger LOG = Logger.getLogger(RoutingTable.class.getName());
	
	private final Key myKey;
	private Bucket[] table = new Bucket[Constants.BITLENGTH];
	
	public RoutingTable(final Key key){
		this.myKey = key;
		for (int i=0; i<Constants.BITLENGTH; i++){
			table[i] = new Bucket();
		}
	}
	
	public Bucket getClosestBucket(final Key key){
		final Key dist = myKey.dist(key);
		LOG.warning("Key:"+key+ " is in bucket:"+dist.rank() + " (myKey:"+myKey+")");
		return table[dist.rank()];
	}
}
