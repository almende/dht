package com.almende.dht;

import java.io.Serializable;
import java.net.URI;

public class Node implements Serializable{
	private static final long serialVersionUID = 941011043741195069L;
	private Key key;
	private URI uri;

	public Node() {
	};
	
	public Node(final Key key, final URI uri){
		this.key=key;
		this.uri=uri;
	}

	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public URI getUri() {
		return uri;
	}

	public void setUri(URI uri) {
		this.uri = uri;
	}
	
	@Override
	public String toString(){
		return key+":"+uri.toASCIIString();
	}
}
