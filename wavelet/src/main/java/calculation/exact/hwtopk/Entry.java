package main.java.calculation.exact.hwtopk;

import java.util.List;
/**
 * 
 * @author dieutth
 * Entry class, represents keyOut (ie. nodeID in the paper) and a list of key-coefficient pair in that node.
 */
public class Entry {
	String keyOut;
	List<IntDouble2> ls;
	
	public Entry(String keyOut, List<IntDouble2> ls) {
		setKeyOut(keyOut);
		setLs(ls);
	}

	public String getKeyOut() {
		return keyOut;
	}

	public void setKeyOut(String keyOut) {
		this.keyOut = keyOut;
	}

	public List<IntDouble2> getLs() {
		return ls;
	}

	public void setLs(List<IntDouble2> ls) {
		this.ls = ls;
	}
	@Override
	public String toString() {
		return keyOut + "--" + ls;
	}
}
