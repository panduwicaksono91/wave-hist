package main.java.calculation.exact.improved;

import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import main.java.calculation.exact1.IntDouble;

public class Phase3Reducer extends RichGroupReduceFunction<Entry, IntDouble> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6019708192064497063L;
	int k;
	HashMap<Integer,Double> map;
	
	public Phase3Reducer(int k) {
		this.k = k;
		map = new HashMap<Integer,Double>();
	}
	
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      List<Row> rows = getRuntimeContext().<Row>getBroadcastVariable("bounds2");
      for (Row row : rows) {
    	  map.put(row.getX(), row.getSx());
      }
    }
	  
	@Override
	public void reduce(Iterable<Entry> values, Collector<IntDouble> out) throws Exception {
		
		for (Entry entry : values) {
			List<IntDouble2> ls = entry.getLs();
			for (int i = 0; i < ls.size(); i++) {
				IntDouble2 id = ls.get(i);
				map.put(id.f0, map.get(id.f0)+ id.f1);
			}
		}
		PriorityQueue<IntDouble> pq = new PriorityQueue<>();
		for (java.util.Map.Entry<Integer, Double> i : map.entrySet()) {
			pq.add(new IntDouble(i.getKey(), i.getValue()));
		}
		
		for (int i = 0; i < k; i++)
			out.collect(pq.poll());
	}

}
