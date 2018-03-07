package main.java.calculation.exact.sendcoef;

import org.apache.flink.api.java.tuple.Tuple2;
/**
 * 
 * @author dieutth, Shibo Cheng
 * Class extends Tuple2 to override compareTo.
 * Mostly used when compute top k coef (with index) from a PriorityQueue
 */
public class IntFloat extends Tuple2<Integer, Float> implements Comparable<IntFloat>{
	/**
	 *
	 */
	private static final long serialVersionUID = -2516847666982801504L;

	public IntFloat() {
		super();
	}
	public IntFloat(Integer i, Float d) {
		super(i, d);
	}
	
	@Override
	public int compareTo(IntFloat o) {
		return -new Float(Math.abs(this.f1)).compareTo(new Float(Math.abs(o.f1)));
	}

}
