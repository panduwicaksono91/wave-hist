package main.java.calculation.exact.sendcoef;

import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

public class TopKReducer implements GroupReduceFunction<IntFloat, IntFloat> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8071436273067587701L;
	private int k;
	public TopKReducer(int k) {
		// TODO Auto-generated constructor stub
		this.k = k;
	}
	@Override
	public void reduce(Iterable<IntFloat> arg0, Collector<IntFloat> arg1) throws Exception {
		// TODO Auto-generated method stub
		PriorityQueue<IntFloat> pq = new PriorityQueue<IntFloat>();
		for (IntFloat t : arg0) {
			pq.add(t);
		}
		for (int i = 0; i < k; i++) {
			arg1.collect(pq.poll());
		}
	}
} 


