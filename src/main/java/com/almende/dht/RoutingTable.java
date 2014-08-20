/*
 * Copyright: Almende B.V. (2014), Rotterdam, The Netherlands
 * License: The Apache Software License, Version 2.0
 */
package com.almende.dht;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The Class RoutingTable.
 */
public class RoutingTable {

	private Key myKey;
	private Bucket[] table = new Bucket[Constants.BITLENGTH];

	/**
	 * Instantiates a new routing table.
	 */
	public RoutingTable() {
	}

	/**
	 * Instantiates a new routing table.
	 *
	 * @param key
	 *            the key
	 */
	public RoutingTable(final Key key) {
		this.myKey = key;
		for (int i = 0; i < Constants.BITLENGTH; i++) {
			table[i] = new Bucket();
		}
	}

	/**
	 * Gets the bucket.
	 *
	 * @param key
	 *            the key
	 * @param offset
	 *            the offset
	 * @return the bucket
	 */
	public Bucket getBucket(final Key key, final int offset) {
		final Key dist = myKey.dist(key);
		final int index = dist.rank() - 1 + offset;
		if (index >= 0 && index < Constants.BITLENGTH) {
			return table[index];
		} else {
			return null;
		}
	}

	/**
	 * Gets the bucket.
	 *
	 * @param key
	 *            the key
	 * @return the bucket
	 */
	public Bucket getBucket(final Key key) {
		return getBucket(key, 0);
	}

	/**
	 * Seen node.
	 *
	 * @param node
	 *            the node
	 */
	public void seenNode(final Node node) {
		final Bucket bucket = getBucket(node.getKey());
		bucket.seenNode(node);
	}

	/**
	 * Gets the closest nodes.
	 *
	 * @param near
	 *            the near
	 * @param limit
	 *            the limit
	 * @param filter
	 *            the filter
	 * @return the closest nodes
	 */
	public Node[] getClosestNodes(final Key near, final int limit,
			final Set<Key> filter) {
		Bucket bucket = getBucket(near);
		Node[] result = bucket.getClosestNodes(near, limit, filter);
		int offset = -1;
		boolean[] edges = new boolean[2];
		edges[0] = false;
		edges[1] = false;
		while (result.length < limit && !(edges[0] && edges[1])) {
			bucket = getBucket(near, offset);
			if (bucket != null) {
				Node[] res = bucket.getClosestNodes(near,
						limit - result.length, filter);
				if (res.length > 0) {
					final Node[] oldres = result;
					result = new Node[oldres.length + res.length];
					for (int i = 0; i < oldres.length; i++) {
						result[i] = oldres[i];
					}
					for (int i = 0; i < res.length; i++) {
						result[i + oldres.length] = res[i];
					}
				}
			} else {
				if (offset < 0) {
					edges[0] = true;
				} else {
					edges[1] = true;
				}
			}
			offset = offset < 0 ? -offset + 1 : -offset;
		}
		return result;
	}

	/**
	 * Gets the closest nodes.
	 *
	 * @param near
	 *            the near
	 * @param limit
	 *            the limit
	 * @param filter
	 *            the filter
	 * @return the closest nodes
	 */
	public Node[] getClosestNodes(final Key near, final int limit,
			final Key[] filter) {
		final Set<Key> set = new HashSet<Key>(filter.length);
		Collections.addAll(set, filter);
		return getClosestNodes(near, limit, set);
	}

	/**
	 * Gets the closest nodes.
	 *
	 * @param near
	 *            the near
	 * @param limit
	 *            the limit
	 * @return the closest nodes
	 */
	public Node[] getClosestNodes(final Key near, final int limit) {
		return getClosestNodes(near, limit, Collections.<Key> emptySet());
	}

	/**
	 * Gets the closest nodes.
	 *
	 * @param near
	 *            the near
	 * @return the closest nodes
	 */
	public Node[] getClosestNodes(final Key near) {
		return getClosestNodes(near, Integer.MAX_VALUE,
				Collections.<Key> emptySet());
	}

	/**
	 * Gets the table.
	 *
	 * @return the table
	 */
	public Bucket[] getTable() {
		return table;
	}

	/**
	 * Sets the table.
	 *
	 * @param table
	 *            the new table
	 */
	public void setTable(Bucket[] table) {
		this.table = table;
	}

	/**
	 * Gets the my key.
	 *
	 * @return the my key
	 */
	public Key getMyKey() {
		return myKey;
	}

	/**
	 * Sets the my key.
	 *
	 * @param myKey
	 *            the new my key
	 */
	public void setMyKey(final Key myKey) {
		this.myKey = myKey;
	}
}
