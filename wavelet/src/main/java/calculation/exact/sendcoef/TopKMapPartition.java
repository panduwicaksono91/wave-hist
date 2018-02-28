package main.java.calculation.exact.sendcoef;

import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

public class TopKMapPartition implements MapPartitionFunction<IntFloat, IntFloat>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6913399449456552933L;
	private int k;
	public TopKMapPartition(int k) {
		this.k = k;
	}
	@Override
	public void mapPartition(Iterable<IntFloat> arg0, Collector<IntFloat> arg1)
			throws Exception {
		// TODO Auto-generated method stub
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

