package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class TestPhase1 {
	
	public static void main(String[] args) {
		
		/*
		 * Test phase 1 from the example in the slide
		 */
		HashMap<Integer, List<Integer>> Fx = new HashMap<Integer, List<Integer>>();
		HashMap<Integer, Double> R = new HashMap<Integer, Double>();
	
		List<Double> kMost = new ArrayList<Double>();
		
		int ind = 0;
		List<List<IntDouble2>> values = new ArrayList<List<IntDouble2>>();
		List<IntDouble2> ls1 = new ArrayList<IntDouble2>();
		List<IntDouble2> ls2 = new ArrayList<IntDouble2>();
		List<IntDouble2> ls3 = new ArrayList<IntDouble2>();
		ls1.add(new IntDouble2(5, 20.0));
		ls1.add(new IntDouble2(3, -30.0));
		
		ls2.add(new IntDouble2(5, 12.0));
		ls2.add(new IntDouble2(6, -20.0));
		
		ls3.add(new IntDouble2(1, 10.0));
		ls3.add(new IntDouble2(6, -10.0));
		values.add(ls1); values.add(ls2); values.add(ls3);
		
		
		for (List<IntDouble2> ls : values) {
			
			for (int i = 0; i < ls.size(); i++) {
			
				IntDouble2 id = ls.get(i);
				int x = id.f0;
				double sx = id.f1;
				
				if (i == 0)
					kMost.add(sx);
				else if (i == ls.size()-1)
					kMost.add(sx);
				
				if (R.containsKey(x)) {
					R.replace(x, R.get(x)+sx);
					Fx.get(x).add(ind);
				}else {
					R.put(x, sx);
					List<Integer> tmp = new ArrayList<Integer>();
					tmp.add(ind);
					Fx.put(x, tmp);
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
				
			T.put(key, ts);
		}
		
	System.out.println();	
	}
}
