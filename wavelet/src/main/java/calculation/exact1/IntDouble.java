package main.java.calculation.exact1;

import org.apache.flink.api.java.tuple.Tuple2;

public class IntDouble extends Tuple2<Integer, Double> implements Comparable<IntDouble>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2516847666982801504L;

	public IntDouble() {
		super();
	}
	public IntDouble(Integer i, Double d) {
		super(i, d);
	}
		// TODO Auto-generated constructor stub
	
		@Override
	public int compareTo(IntDouble o) {
		// TODO Auto-generated method stub
		return -new Double(Math.abs(this.f1)).compareTo(new Double(Math.abs(o.f1)));
	}

}
