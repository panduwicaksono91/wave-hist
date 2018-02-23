package main.java.calculation.exact.improved;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class Phase1Mapper implements MapFunction<String, Entry>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4937400236372352186L;
	private int U;
	private int numLevels;
	private int k; 
	private String folder;
	public List<IntDouble2> result;
	
	public Phase1Mapper(int U, int numLevels, int k, String folder) {
		this.U = U;
		this.numLevels = numLevels;
		this.k = k;
		this.folder = folder;
		result = new ArrayList<IntDouble2>();
	}

	@Override
	public Entry map(String value) throws Exception {
		String keyOut = DigestUtils.shaHex(value);
		PrintWriter printWriter = new PrintWriter(folder + keyOut);
		printWriter.write(keyOut+"_");
		Map<Integer, Double> histo = new HashMap<Integer, Double>();
		
		
		String[] tokens = value.split(",");
		for (String s : tokens) {
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
					double a = histo.get(j+1), b = histo.get(j);
					detailCo = (a - b)/2;
					avgCo = (a + b)/2;
					detailCoefficients.put(ind, detailCo);
					temp.put(j/2, avgCo);
					
				}else if (histo.containsKey(j)) {
					double b = histo.get(j);
					detailCo = -b/2;
					avgCo = b/2;
					detailCoefficients.put(ind, detailCo);
					temp.put(j/2, avgCo);
					
				}else if (histo.containsKey(j+1)) {
					
					detailCo = histo.get(j+1)/2;
					avgCo = detailCo;
					detailCoefficients.put(ind, detailCo);
					temp.put(j/2, avgCo);
				}
			}
			
			histo.clear();
			histo.putAll(temp);
		}
		
		//selecting top k
		PriorityQueue<IntDouble2> pq = new PriorityQueue<IntDouble2>();
		for (Integer i : detailCoefficients.keySet())
			pq.add(new IntDouble2(i, detailCoefficients.get(i)));
		
		if (histo.containsKey(0)) pq.add(new IntDouble2(1, histo.get(0)));
		
		
		for (int i = 0; i < k; i++) {
			IntDouble2 val = pq.poll();
			if (val == null)
				break;
			if (val.f1 >= 0) {
				printWriter.write(val.f0 + ":" + val.f1 + ",");
				break;
			}
			result.add(val);
			
		}
		for (int i = pq.size(); i > k; i--) {
			IntDouble2 val = pq.poll();
			if (val == null)
				break;
			printWriter.write(val.f0 + ":" + val.f1 + ",");
		}
		
		while (!pq.isEmpty()) {
			IntDouble2 val = pq.poll();
			if (val.f1 <= 0) {
				printWriter.write(val.f0 + ":" + val.f1 + ",");
				continue;
			}
			result.add(val);
			
		}
		printWriter.close();
		return new Entry(keyOut, result);
	}
}
