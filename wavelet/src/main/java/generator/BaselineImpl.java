package main.java.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BaselineImpl {
	public static void buildWaveletWithoutFlink() {
		Double[] freq = new Double[] {(double) 3, (double) 5, (double) 10, (double) 8, (double) 2, (double) 2, (double) 10, (double) 14};
		int numLevels = (int)(Math.log(freq.length)/Math.log(2));
		
		List<Double> avgCoefficients = new ArrayList<Double>(Arrays.asList(freq));
		
		List<Double> detailCoefficients = new ArrayList<Double>();
		List<Double> temp;

		for (int i = 0; i < numLevels; i++) {
			temp = new ArrayList<Double>();
			for (int j = 0; j < avgCoefficients.size(); j+=2) {
				double detailCo = (avgCoefficients.get(j+1) - avgCoefficients.get(j))/2;
				double avgCo = (avgCoefficients.get(j+1) + avgCoefficients.get(j))/2;
				detailCoefficients.add(detailCo);
				temp.add(avgCo);
			}
			avgCoefficients = temp;
		}
		
		for (Double i : detailCoefficients)
			System.out.println(i);
	}
	
	public static void main(String[] args) throws Exception {
		//file content: "7,8,1,8,1,8,7,8,3,2,7,7,3,4,7,2,3,4,8,7,4,4,2,6,8,8,8,4,4,8,4,8,3,5,6,8,7,3,4,3,8,8,2,2,3,1,3,7,3,3,7,7,5,8"
		//this produce the histogram in domain u=8 as described in the paper (figure 1)
		String inputFile = "D:\\data\\bdapro\\wavelet\\toydataset_1.txt";
		int U = 8;
		int numLevels = (int)(Math.log(U)/Math.log(2));
				
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Integer, Double>> freqs = env.readTextFile(inputFile)
								  .flatMap(new FlatMapFunction<String, Tuple2<Integer, Double>>() {
									@Override
									public void flatMap(String arg0, Collector<Tuple2<Integer, Double>> arg1) throws Exception {
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
											arg1.collect(new Tuple2<Integer, Double>(i, detailCoefficients.get(i)));
										
										arg1.collect(new Tuple2<Integer, Double>(1, histo.get(0)));
									}
								}).groupBy(0).sum(1);
		freqs.print();
	}
}
