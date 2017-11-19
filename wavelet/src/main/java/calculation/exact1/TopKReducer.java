package main.java.calculation.exact1;

import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

public class TopKReducer implements GroupReduceFunction<IntDouble, IntDouble> {
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
	public void reduce(Iterable<IntDouble> arg0, Collector<IntDouble> arg1) throws Exception {
		// TODO Auto-generated method stub
		PriorityQueue<IntDouble> pq = new PriorityQueue<IntDouble>();
		for (IntDouble t : arg0) {
			pq.add(t);
		}
		for (int i = 0; i < k; i++) {
			arg1.collect(pq.poll());
		}
	}
} 


