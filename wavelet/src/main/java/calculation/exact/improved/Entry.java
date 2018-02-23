package main.java.calculation.exact.improved;

import java.util.List;

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
