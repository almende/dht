/*
 * Copyright: Almende B.V. (2014), Rotterdam, The Netherlands
 * License: The Apache Software License, Version 2.0
 */
package com.almende.dht.rpc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.almende.dht.Constants;
import com.almende.dht.Key;
import com.almende.dht.Node;
import com.almende.dht.RoutingTable;
import com.almende.dht.TimedValue;
import com.almende.eve.transform.rpc.annotation.Access;
import com.almende.eve.transform.rpc.annotation.AccessType;
import com.almende.eve.transform.rpc.annotation.Name;
import com.almende.eve.transform.rpc.annotation.Namespace;
import com.almende.util.jackson.JOM;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The Class DHT Adapter.
 */
@Namespace("dht")
public class DHT {
	// TODO: Make this persistent
	private RoutingTable				rt		= new RoutingTable(Key.random());
	private Map<Key, Set<TimedValue>>	values	= new HashMap<Key, Set<TimedValue>>();

	/**
	 * Ping. (Kademlia's PING)
	 *
	 * @return the boolean
	 */
	@Access(AccessType.PUBLIC)
	public Boolean ping() {
		return true;
	}

	/**
	 * Store the value at the key. (Kademlia's STORE)
	 *
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 */
	@Access(AccessType.PUBLIC)
	public void store(@Name("key") Key key, @Name("value") ObjectNode value) {
		Set<TimedValue> current = new TreeSet<TimedValue>();
		TimedValue tv = new TimedValue(value);

		if (values.containsKey(key)) {
			current = values.get(key);
		}
		current.remove(tv);
		current.add(tv);
		values.put(key, current);
	}

	/**
	 * Delete a specific value from this key.
	 *
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 */
	@Access(AccessType.PUBLIC)
	public void delete(@Name("key") Key key, @Name("value") ObjectNode value) {
		if (values.containsKey(key)) {
			Set<TimedValue> current = values.remove(key);
			TimedValue tv = new TimedValue(value);

			current.remove(tv);
			if (current.size() > 0) {
				values.put(key, current);
			}
		}

	}

	/**
	 * Delete a specific value from this key.
	 *
	 * @param key
	 *            the key
	 */
	@Access(AccessType.PUBLIC)
	public void deleteAll(@Name("key") Key key) {
		values.remove(key);
	}

	/**
	 * Find_close_nodes. (Kademlia's FIND_NODE)
	 *
	 * @param key
	 *            the key
	 * @return An objectNode containing {"nodes":[.....]}
	 */
	@Access(AccessType.PUBLIC)
	public ObjectNode find_close_nodes(@Name("key") Key key) {
		final ObjectNode result = JOM.createObjectNode();
		result.set(
				"nodes",
				JOM.getInstance().valueToTree(
						rt.getClosestNodes(key, Constants.K)));
		return result;
	}

	/**
	 * Find_value get latest value
	 *
	 * @param key
	 *            the key
	 * @return And objectNode containing either: {"value":....} or
	 *         {"nodes":[....]}
	 */
	@Access(AccessType.PUBLIC)
	public ObjectNode find_value(@Name("key") Key key) {
		if (values.containsKey(key)) {
			final ObjectNode result = JOM.createObjectNode();
			result.set("value", values.get(key).iterator().next().getValue());
			return result;
		} else {
			return find_close_nodes(key);
		}
	}

	/**
	 * Find_values get all values for key
	 *
	 * @param key
	 *            the key
	 * @return And objectNode containing either: {"value":....} or
	 *         {"nodes":[....]}
	 */
	@Access(AccessType.PUBLIC)
	public ObjectNode find_values(@Name("key") Key key) {
		if (values.containsKey(key)) {
			final ObjectNode result = JOM.createObjectNode();
			final ArrayNode arr = JOM.createArrayNode();
			final Set<TimedValue> current = values.get(key);
			final int size = current.size();
			final Iterator<TimedValue> iter = values.get(key).iterator();
			while (iter.hasNext()) {
				final TimedValue item = iter.next();
				if (item.getTtl() > 0) {
					arr.add(item.getValue());
				} else {
					iter.remove();
				}
			}
			if (current.size() < size) {
				values.put(key, current);
			}
			result.set("values", arr);
			return result;
		} else {
			return find_close_nodes(key);
		}
	}

	/**
	 * Iterative_node_lookup.
	 *
	 * @param key
	 *            the key
	 * @return the object node
	 */
	public ObjectNode iterative_node_lookup(Key key) {
		return JOM.createObjectNode();
	}

	/**
	 * Iterative_find_value.
	 *
	 * @param key
	 *            the key
	 * @return the object node
	 */
	public ObjectNode iterative_find_value(Key key) {
		return JOM.createObjectNode();
	}

	/**
	 * Join the network
	 *
	 * @param remote
	 *            the pre-known remote start node
	 */
	@Access(AccessType.PUBLIC)
	public void join(@Name("remote") Node remote) {
		
		
	}

	/**
	 * Refresh buckets (scheduled at tRefresh)
	 */
	@Access(AccessType.PUBLIC)
	public void refresh() {}

	//Schedule expiry
}
