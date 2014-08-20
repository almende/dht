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

public class Bucket {
	private static final Logger LOG = Logger.getLogger(Bucket.class.getName());

	private LinkedHashMap<Key, Node> nodes;

	public Bucket() {
		nodes = new LinkedHashMap<Key, Node>(Constants.K);
	};

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

	public Node[] getClosestNodes(final Key near, final int limit,
			final Key[] filter) {
		final Set<Key> set = new HashSet<Key>(filter.length);
		Collections.addAll(set, filter);
		return getClosestNodes(near, limit, set);
	}

	public Node[] getClosestNodes(final Key near, final int limit) {
		return getClosestNodes(near, limit, Collections.<Key> emptySet());
	}

	public Node[] getClosestNodes(final Key near) {
		return getClosestNodes(near, Integer.MAX_VALUE,
				Collections.<Key> emptySet());
	}

	public LinkedHashMap<Key, Node> getNodes() {
		return nodes;
	}

	public void setNodes(LinkedHashMap<Key, Node> nodes) {
		this.nodes = nodes;
	}
}
