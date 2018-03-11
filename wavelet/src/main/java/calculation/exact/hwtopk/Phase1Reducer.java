package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * 
 * @author dieutth
 * Reducer of phase 1 of hwtopk.
 */
public class Phase1Reducer implements GroupReduceFunction<Entry, Row>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3616111236540300386L;
	int k;
	
	public Phase1Reducer(int k) {
		this.k = k;
	}
	
	/**
	 * Compute table R from top k most negative, positive coefs 
	 * sent from all nodes.
	 * See histogram slide for how to compute table R.
	 * @param values list of entry; each entry contains list of top k most negative, positive coefs, and nodeID (keyOut).
	 * @param out collect list of computed Row of table R
	 */
	@Override
	public void reduce(Iterable<Entry> values, Collector<Row> out) throws Exception {
		HashMap<Integer, List<Integer>> Fx = new HashMap<Integer, List<Integer>>();
		HashMap<Integer, Double> R = new HashMap<Integer, Double>();
		HashMap<Integer, ArrayList<String>> keyMap = new HashMap<Integer, ArrayList<String>>();
		List<Double> kMost = new ArrayList<Double>();
		List<String> mappers = new ArrayList<String>();
		int ind = 0;
	
		for (Entry entry : values) {
			
			List<IntDouble2> ls = entry.getLs();
			String keyOut = entry.getKeyOut();
			mappers.add(keyOut);
			for (int i = 0; i < ls.size(); i++) {
			
				IntDouble2 id = ls.get(i);
				int x = id.f0;
				double sx = id.f1;
				
				if (i == 0) {
					if (sx < 0)
						kMost.add(0.0);
					else kMost.add(sx);
				}
				if (i == ls.size()-1) {
					if (sx > 0)
						kMost.add(0.0);
					else kMost.add(sx);
				}
				
				if (R.containsKey(x)) {
					R.replace(x, R.get(x)+sx);
					Fx.get(x).add(ind);
					keyMap.get(x).add(keyOut);
				}else {
					R.put(x, sx);
					List<Integer> tmp = new ArrayList<Integer>();
					tmp.add(ind);
					Fx.put(x, tmp);
					
					ArrayList<String> nodes = new ArrayList<String>();
					nodes.add(keyOut);
					keyMap.put(x, nodes);
				}
			}
			ind++;
		}
		
		HashMap<Integer, double[]> T = new HashMap<Integer, double[]>();
				
		for (Integer key : Fx.keySet()) {
			List<Integer> value = Fx.get(key);
			double[] ts = new double[] {R.get(key), R.get(key), 0};
			
			for (int i = 0; i < ind; i++) {
				if (!value.contains(i)) {
					ts[0] += kMost.get(2*i);
					ts[1] += kMost.get(2*i+1);
				}
			}
			if ((ts[0] < 0 && ts[1] < 0))
				ts[2] = -(ts[0] < ts[1]? ts[1] : ts[0]);
			else if ((ts[0] > 0 && ts[1] > 0))
				ts[2] = ts[0] > ts[1] ? ts[1] : ts[0];
				
			out.collect(new Row(key, R.get(key), ts[0], ts[1], ts[2], keyMap.get(key)));
		}
		
		
	}


}
