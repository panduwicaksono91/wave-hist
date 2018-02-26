package main.java.calculation.exact.hwtopk;

import java.util.List;

public class Row {
	int x;
	double sx, tplus, tminus, t;
	List<String> nodes;	
	public Row(int x, double sx, double tplus, double tminus, double t, List<String>  nodes) {
		this.x = x;
		this.sx = sx;
		this.tplus = tplus;
		this.tminus = tminus;
		this.t = t;
		this.nodes = nodes;
	}

	public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
	}

	public double getSx() {
		return sx;
	}

	public void setSx(double sx) {
		this.sx = sx;
	}

	public double getTplus() {
		return tplus;
	}

	public void setTplus(double tplus) {
		this.tplus = tplus;
	}

	public double getTminus() {
		return tminus;
	}

	public void setTminus(double tminus) {
		this.tminus = tminus;
	}

	public double getT() {
		return t;
	}

	public void setT(double t) {
		this.t = t;
	}

	public List<String>  getNode() {
		return nodes;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return x + "," + sx + "," + tplus + "," + tminus + "," + t + "," + String.join(",", nodes) + "\n";
	}
}
