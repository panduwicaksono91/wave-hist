package main.java.calculation.exact.sendcoef;

import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
/**
 * 
 * @author dieutth
 * A GroupReduce function to select top k IntFloat values in a list of IntFloat values
 */
public class TopKReducer implements GroupReduceFunction<IntFloat, IntFloat> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8071436273067587701L;
	private int k;
	public TopKReducer(int k) {
		this.k = k;
	}
	@Override
	public void reduce(Iterable<IntFloat> arg0, Collector<IntFloat> arg1) throws Exception {
		PriorityQueue<IntFloat> pq = new PriorityQueue<IntFloat>();
		for (IntFloat t : arg0) {
			pq.add(t);
		}
		
		int count = k;
		while (!pq.isEmpty() && count > 0) {
			arg1.collect(pq.poll());
			count--;
		}
	}
} 


