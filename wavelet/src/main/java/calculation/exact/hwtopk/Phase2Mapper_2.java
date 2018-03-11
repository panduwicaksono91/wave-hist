package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 
 * @author dieutth
 * Mapper of phase 2 of HWTopK
 */
public class Phase2Mapper_2 extends RichMapFunction<String, Entry>{
	
	private static final long serialVersionUID = -3549184163020471119L;
	private double bound;
	String keyOut;
	int k;
	public Phase2Mapper_2(String keyOut, int k) {
		this.keyOut = keyOut;
		this.k = k;
	}
	
	
	public Phase2Mapper_2(int k) {
		this.k = k;
	}
	
	/**
	 * Compute the bound from broadcast variable 
	 */
   @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
     List<Row> rows = getRuntimeContext().<Row>getBroadcastVariable("bounds");
      PriorityQueue<Double> pq = new PriorityQueue<>(rows.size());
      for (Row row : rows) {
    	  pq.add(row.getT());
      }

      for (int i = 0; i < rows.size() - k; i++)
    	  pq.poll();
      
      bound = pq.poll();
    }

   
   /**
    * Compute all local coefs that has absolute value smaller than bound.
    * These local coefs, together with its keyOut (node ID), will be wrapped in an Entry and 
    * sent to reducer.
    */
	@Override
	public Entry map(String value) throws Exception {
		
		String[] tokens = value.split("_");
		String keyOut = tokens[0];
		String[] tuples = tokens[1].split(";");
		List<IntDouble2> result = new ArrayList<IntDouble2>();
		
		for (String tupleStr : tuples) {
			String[] tmp = tupleStr.split(",");
			int k_code = Integer.valueOf(tmp[0]);
//			keyOut = tmp[0];
			if (k_code == 0) {
				double coef = Float.valueOf(tmp[2]).doubleValue();
				if (coef > bound || coef < -bound)
					result.add(new IntDouble2( Integer.valueOf(tmp[1]), coef));
			}
		}

		return new Entry(keyOut, result);
	}

}
