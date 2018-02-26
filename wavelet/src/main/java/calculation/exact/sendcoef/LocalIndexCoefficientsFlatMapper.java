package main.java.calculation.exact1;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class LocalIndexCoefficientsFlatMapper implements FlatMapFunction<String, IntDouble>{
	
	private static final long serialVersionUID = -3549184163020471119L;
	
	private int U;
	private int numLevels;
	
	public LocalIndexCoefficientsFlatMapper(int U, int numLevels) {
		this.U = U;
		this.numLevels = numLevels;
	}

	@Override
	public void flatMap(String arg0, Collector<IntDouble> arg1) throws Exception {
		// TODO Auto-generated method stub
		Map<Integer, Double> histo = new HashMap<Integer, Double>();
		
		for (String s : arg0.split(",")) {
			int key = Integer.valueOf(s)-1;
			if (histo.containsKey(key)) {
				double val = histo.get(key)+1;
				histo.put(key, val);
			}else {
				histo.put(key, 1.0);
			}
		}
		
		HashMap<Integer, Double> detailCoefficients = new HashMap<Integer, Double>();
		HashMap<Integer, Double> temp;
		
		double detailCo;
		double avgCo;
		for (int i = 0; i < numLevels; i++) {
			temp = new HashMap<Integer, Double>();
			int baseInd = (int)(U/Math.pow(2, i+1) + 1);
			
			for (int j = 0; j < U/(Math.pow(2, i)); j+=2) {
				int ind = baseInd + j/2;
				if (histo.containsKey(j) && histo.containsKey(j+1)) {
					detailCo = (histo.get(j+1) - histo.get(j))/2;
					avgCo = (histo.get(j+1) + histo.get(j))/2;
					detailCoefficients.put(ind, detailCo);
					temp.put(j/2, avgCo);
					
				}else if (histo.containsKey(j)) {
					detailCo = -histo.get(j)/2;
					avgCo = histo.get(j)/2;
					detailCoefficients.put(ind, detailCo);
					temp.put(j/2, avgCo);
					
				}else if (histo.containsKey(j+1)) {
					detailCo = histo.get(j+1)/2;
					avgCo = histo.get(j+1)/2;
					detailCoefficients.put(ind, detailCo);
					temp.put(j/2, avgCo);
				}
			}
			
			histo.clear();
			histo.putAll(temp);
		}
		
		for (Integer i : detailCoefficients.keySet())
			arg1.collect(new IntDouble(i, detailCoefficients.get(i)));
		
		arg1.collect(new IntDouble(1, histo.get(0)));
	}
}
