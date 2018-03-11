package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 
 * @author dieutth
 * Legacy class.
 * Mapper of Phase 2 of HWTopK, when phase 1 write to file local coefs.
 * Writing to file is not a correct way to implement HWTopK so
 * this mapper is not going to be used.
 */
public class Phase2Mapper extends RichMapFunction<String, Entry>{
	
	private static final long serialVersionUID = -3549184163020471119L;
	private double bound;
	String keyOut;
	int k;
	public Phase2Mapper(String keyOut, int k) {
		this.keyOut = keyOut;
		this.k = k;
	}
	public Phase2Mapper(int k) {
		this.k = k;
	}
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

	@Override
	public Entry map(String value) throws Exception {
		String kO = value.split("_")[0];
		
		String[] tokens = value.split("_")[1].split(",");
		List<IntDouble2> ls = new ArrayList<IntDouble2>();
		for (int i = 0; i < tokens.length-1; i++) {
			String[] item = tokens[i].split(":");
			double d = Double.valueOf(item[1]);
			if (d < -bound)
				ls.add(new IntDouble2(Integer.valueOf(item[0]), d));
			else
				break;
		}
		
		for (int i = tokens.length-2; i >= 0; i--) {
			String[] item = tokens[i].split(":");
			double d = Double.valueOf(item[1]);
			if (d > bound)
				ls.add(new IntDouble2(Integer.valueOf(item[0]), d));
			else
				break;
		}
		
		return new Entry(kO, ls);
	}

}
