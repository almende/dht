/*
 * Copyright: Almende B.V. (2014), Rotterdam, The Netherlands
 * License: The Apache Software License, Version 2.0
 */
package com.almende.test.dht;

import java.io.IOException;
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * The Class TestAgent.
 */
public class TestScale extends TestCase {

	private static final int	NOFNODES	= 1000;

	/**
	 * Test a large nof nodes.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws URISyntaxException
	 *             the URI syntax exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	@Test
	public void testScale() throws IOException, URISyntaxException,
			InterruptedException {
		DHTAgent[] agents = new DHTAgent[NOFNODES];
		
		DHTAgent agent = new DHTAgent("agent_0");
		agents[0] = agent;
		for (int i = 1; i < NOFNODES; i++) {
			System.out.println("Created node:agent_"+i);
			DHTAgent next = new DHTAgent("agent_" + i);
			try {
				next.getDht().join(agent.asNode());
			} catch (NullPointerException e) {
				System.err.println("NPE at:" + i);
				throw e;
			}
			agent = next;
			agents[i] = agent;
		}
	}
}
