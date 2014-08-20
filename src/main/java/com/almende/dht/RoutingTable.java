package com.almende.dht;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RoutingTable {

	private Key myKey;
	private Bucket[] table = new Bucket[Constants.BITLENGTH];

	public RoutingTable() {
	}

	public RoutingTable(final Key key) {
		this.myKey = key;
		for (int i = 0; i < Constants.BITLENGTH; i++) {
			table[i] = new Bucket();
		}
	}

	public Bucket getBucket(final Key key, final int offset) {
		final Key dist = myKey.dist(key);
		final int index = dist.rank() - 1 + offset;
		if (index >= 0 && index < Constants.BITLENGTH) {
			return table[index];
		} else {
			return null;
		}
	}

	public Bucket getBucket(final Key key) {
		return getBucket(key, 0);
	}

	public void seenNode(final Node node) {
		final Bucket bucket = getBucket(node.getKey());
		bucket.seenNode(node);
	}

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

	public Bucket[] getTable() {
		return table;
	}

	public void setTable(Bucket[] table) {
		this.table = table;
	}

	public Key getMyKey() {
		return myKey;
	}

	public void setMyKey(final Key myKey) {
		this.myKey = myKey;
	}
}
