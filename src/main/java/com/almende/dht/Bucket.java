/*
 * Copyright: Almende B.V. (2014), Rotterdam, The Netherlands
 * License: The Apache Software License, Version 2.0
 */
package com.almende.dht;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class Bucket.
 */
public class Bucket {
	private static final Logger LOG = Logger.getLogger(Bucket.class.getName());

	private LinkedHashMap<Key, Node> nodes;

	/**
	 * Instantiates a new bucket.
	 */
	public Bucket() {
		nodes = new LinkedHashMap<Key, Node>(Constants.K);
	};

	/**
	 * Seen node.
	 *
	 * @param node
	 *            the node
	 */
	public void seenNode(final Node node) {
		synchronized (nodes) {
			if (nodes.containsKey(node.getKey())) {
				nodes.remove(node.getKey());
				nodes.put(node.getKey(), node);
			} else if (nodes.size() < Constants.K) {
				nodes.put(node.getKey(), node);
			} else {
				// get first Node through iterator, do Ping(), wait, on answer
				// within timeout drop key; else drop first Node, recurse
				// seenNode();
				LOG.log(Level.WARNING, "Still not implemented!",
						new NoSuchElementException());
			}
		}
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
		synchronized (nodes) {
			final TreeMap<Key, Node> distMap = new TreeMap<Key, Node>();
			final Iterator<Entry<Key, Node>> iter = nodes.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<Key, Node> entry = iter.next();
				if (filter != null && filter.contains(entry.getKey())) {
					continue;
				}
				distMap.put(near.dist(entry.getKey()), entry.getValue());
			}
			final Node[] values = distMap.values().toArray(new Node[0]);
			return Arrays.copyOf(values, Math.min(limit, distMap.size()));
		}
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
	 * Gets the nodes.
	 *
	 * @return the nodes
	 */
	public LinkedHashMap<Key, Node> getNodes() {
		return nodes;
	}

	/**
	 * Sets the nodes.
	 *
	 * @param nodes
	 *            the nodes
	 */
	public void setNodes(LinkedHashMap<Key, Node> nodes) {
		this.nodes = nodes;
	}
}
