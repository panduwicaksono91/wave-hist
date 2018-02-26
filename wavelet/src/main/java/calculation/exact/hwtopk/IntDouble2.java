package main.java.calculation.exact.improved;

import org.apache.flink.api.java.tuple.Tuple2;

public class IntDouble2 extends Tuple2<Integer, Double> implements Comparable<IntDouble2>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2516847666982801504L;

	public IntDouble2() {
		super();
	}
	public IntDouble2(Integer i, Double d) {
		super(i, d);
	}
		// TODO Auto-generated constructor stub
	
		@Override
	public int compareTo(IntDouble2 o) {
		// TODO Auto-generated method stub
		return this.f1.compareTo(o.f1);
	}

}
