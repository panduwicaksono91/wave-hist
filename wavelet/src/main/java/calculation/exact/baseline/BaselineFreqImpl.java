package main.java.calculation.exact.baseline;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import main.java.calculation.exact1.IntDouble;

public class BaselineFreqImpl {
	public static void main(String[] args) {
		String inputFile = "src\\resource\\toydataset_1.txt";
		//domain U
		int U = 8;
		//number of Levels of the wavelet tree
		int numLevels = (int)(Math.log(U)/Math.log(2));
		//desired number of coefficients to keep
		int k = 3;
	
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<IntDouble> freqs = env.readTextFile(inputFile)
									.flatMap( new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
								            @Override
								            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
								                // normalize and split the line
								                String[] tokens = value.split("\\W+|,");
								
								                // emit the pairs
								                for (String token : tokens) {
								                    if (token.length() > 0) {
								                        out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(token), 1));
								                    }
								                }
								            }
								        }
									).groupBy(0)
									.sum(1)
									.reduceGroup(new GroupReduceFunction<Tuple2<Integer,Integer>, IntDouble>() {

										@Override
										public void reduce(Iterable<Tuple2<Integer, Integer>> arg0, Collector<IntDouble> arg1)
												throws Exception {
											
											Map<Integer, Double> histo = new HashMap<Integer, Double>();
											for (Tuple2<Integer, Integer> t : arg0) {
												histo.put(t.f0-1, new Double(t.f1));
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
											
											
											//selecting top k
											PriorityQueue<IntDouble> pq = new PriorityQueue<IntDouble>();
											for (Integer i : detailCoefficients.keySet())
												pq.add(new IntDouble(i, detailCoefficients.get(i)));
											
											pq.add(new IntDouble(1, histo.get(0)));
											
											for (int i = 0; i < k; i++) {
												arg1.collect(pq.poll());
											}
											
										}
									});
		try {
			freqs.print();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
