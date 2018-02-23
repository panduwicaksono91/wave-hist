package main.java.calculation.exact.improved;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class Phase2Reducer extends RichGroupReduceFunction<Entry, Row> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4403117547914700204L;
	int k;
	List<Row> rows;
	private double bound;
	public Phase2Reducer(int k) {
		this.k = k;
	}
	
   @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rows = getRuntimeContext().<Row>getBroadcastVariable("bounds");
      PriorityQueue<Double> pq = new PriorityQueue<>(rows.size());
      for (Row row : rows) {
    	  pq.add(row.getT());
      }

      for (int i = 0; i < rows.size() - k; i++)
    	  pq.poll();
      
      bound = pq.poll();
    }

   @Override
	public void reduce(Iterable<Entry> values, Collector<Row> out) throws Exception {
	   HashMap<Integer, Row> map = new HashMap<Integer, Row>();
	   Set<String> s = new HashSet<>();
	   for (Row row : rows) {
		   map.put(row.getX(), row);
		   s.addAll(row.nodes);
	   }
	   for (Entry entry : values) {
		   List<IntDouble2> ls = entry.getLs();
		   String keyOut = entry.getKeyOut();
		   for (int i = 0; i < ls.size(); i++) {
				IntDouble2 id = ls.get(i);
				if (map.containsKey(id.f0)) {
					Row row = map.get(id.f0);
					row.setSx(row.getSx() + id.f1);
					row.setTplus(row.getTplus() + id.f1);
					row.setTminus(row.getTminus() + id.f1);
					row.getNode().add(keyOut);
				}else {
					List<String> list = new ArrayList<String>();
					list.add(keyOut);
					rows.add(new Row(id.f0, id.f1, id.f1, id.f1, 0, list));
				}
		   }
	   }
	   
	   for (Row row : rows) {
//		   row.setTminus(row.getTminus() - (s.size() - row.getNode().size())*bound);
//		   row.setTplus(row.getTplus() + (s.size() - row.getNode().size())*bound);
//		   if (Math.abs(row.getTminus()) > Math.abs(row.getTplus()))
		   out.collect(row);
	   }
		
	}
}
