package main.java.calculation.exact1;

import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

public class TopKMapPartition implements MapPartitionFunction<IntDouble, IntDouble>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6913399449456552933L;
	private int k;
	public TopKMapPartition(int k) {
		this.k = k;
	}
	@Override
	public void mapPartition(Iterable<IntDouble> arg0, Collector<IntDouble> arg1)
			throws Exception {
		// TODO Auto-generated method stub
		PriorityQueue<IntDouble> pq = new PriorityQueue<IntDouble>();
		for (IntDouble t : arg0) {
			pq.add(t);
			
		}
		int count = k;
		while (!pq.isEmpty() && count > 0) {
		
			arg1.collect(pq.poll());
		}
		
	}
}

