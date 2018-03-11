package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 
 * @author dieutth
 * Mapper of phase 3 of HWTopK
 *
 */
public class Phase3Mapper_2  extends RichMapFunction<String, Entry>{

	private static final long serialVersionUID = 4079441085449335887L;
//	String keyOut;
	List<Row> rows;
//	public Phase3Mapper(String keyOut) {
//		this.keyOut = keyOut;
//	}
//	
	/**
	 * Get current table R (see histogram slide for table R) from broadcast variable
	 */
	 @Override
	    public void open(Configuration parameters) throws Exception {
	      super.open(parameters);
	      rows = getRuntimeContext().<Row>getBroadcastVariable("bounds2");
	    }
	 
	 /**
	  * Send all coef table R needs from this mapper. All coefs are wrapped in an Entry together with nodeID (keyOut)
	  */
	@Override
	public Entry map(String value) throws Exception {
		String[] tokens = value.split("_");
		String keyOut = tokens[0];
		String[] tuples = tokens[1].split(";");
		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
		
		for (String tupleStr : tuples) {
			String[] tmp = tupleStr.split(",");
			int ind = Integer.valueOf(tmp[1]);
			double coef = Double.valueOf(tmp[2]);
			int k_code = Integer.valueOf(tmp[0]);
			if (k_code == 0)
				map.put(ind, coef);
		}
		
		List<IntDouble2> ls = new ArrayList<IntDouble2>();
		
		for (Row row : rows) {
//			List<String> nodes = row.getNode();
			if (!row.getNode().contains(keyOut)) {
				int key = row.getX();
				if (map.containsKey(key))
					ls.add(new IntDouble2(key, map.get(key)));
			}
		}
		
		return new Entry(keyOut, ls);
	}

}
