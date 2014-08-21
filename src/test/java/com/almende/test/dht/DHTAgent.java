/*
 * Copyright: Almende B.V. (2014), Rotterdam, The Netherlands
 * License: The Apache Software License, Version 2.0
 */
package com.almende.test.dht;

import com.almende.dht.rpc.DHT;
import com.almende.eve.agent.Agent;
import com.almende.eve.transform.rpc.annotation.Namespace;

/**
 * The Class DHTAgent.
 */
public class DHTAgent extends Agent {
	//TODO: make DHT a capability
	private DHT dht = new DHT();
	
	/**
	 * Gets the dht.
	 *
	 * @return the dht
	 */
	@Namespace("*")
	public DHT getDht(){
		return dht;
	}
	
	
}
