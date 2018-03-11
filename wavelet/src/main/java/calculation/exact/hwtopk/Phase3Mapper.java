package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 
 * @author dieutth
 * Legacy class.
 * Mapper of Phase 3 of HWTopK, when phase 1 write to file local coefs.
 * Writing to file is not a correct way to implement HWTopK so
 * this mapper is not going to be used.
 */
public class Phase3Mapper  extends RichMapFunction<String, Entry>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4079441085449335887L;
//	String keyOut;
	List<Row> rows;
//	public Phase3Mapper(String keyOut) {
//		this.keyOut = keyOut;
//	}
//	
	 @Override
	    public void open(Configuration parameters) throws Exception {
	      super.open(parameters);
	      rows = getRuntimeContext().<Row>getBroadcastVariable("bounds2");
	    }
	 
	@Override
	public Entry map(String value) throws Exception {
		String kO = value.split("_")[0];
		
		String[] tokens = value.split("_")[1].split(",");
		List<IntDouble2> ls = new ArrayList<IntDouble2>();
		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
		
		for (int i = 0; i < tokens.length-1; i++) {
			String[] item = tokens[i].split(":");
			map.put(Integer.valueOf(item[0]), Double.valueOf(item[1]));
		}
		for (Row row : rows) {
//			List<String> nodes = row.getNode();
			if (!row.getNode().contains(kO)) {
				int key = row.getX();
				if (map.containsKey(key))
					ls.add(new IntDouble2(key, map.get(key)));
			}
		}
		
		return new Entry(kO, ls);
	}

}
