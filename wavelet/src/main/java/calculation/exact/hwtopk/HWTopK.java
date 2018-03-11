package main.java.calculation.exact.hwtopk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import main.java.calculation.exact.sendcoef.IntDouble;
/**
 * 
 * @author dieutth
 * Implementation of HWTopK algorithm.
 */

public class HWTopK {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//Parameters for testing in cluster
		String inputFile = "/share/flink/tmp/dataset245.txt";
		int U = (int)Math.pow(2, 29);
		int numLevels = (int)(Math.log(U)/Math.log(2));
		int k = 30;
//		String folder = "/share/flink/tmp/mapper/";
		
		//Parameters for testing in local
//		String inputFile = "D:\\gitworkspace\\wave-hist\\wavelet\\src\\resource\\toydataset_1.txt";
//		int U = 8;
//		int numLevels = (int)(Math.log(U)/Math.log(2));
//		int k = 5;
		
		/**
		 * Phase 1 - mapper: Compute all local coefs and store result as a dataset of string
		 * (instead of writing to file). This manages sync: phase 2 starts after phase 1, but 
		 * the dataset of string is big and we still need to send a significant amount of data.
		 */
		DataSet<String> phase1 =
				env.readTextFile(inputFile)
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 3754795380866684163L;

					@Override
					public void flatMap(String value, Collector<String> out)
							throws Exception {
						String keyOut = DigestUtils.shaHex(value);
						
						float[] histo = new float[U];
						for (String s : value.split(",")) {
							int val = Integer.valueOf(s);
							histo[val-1] += 1;
						}
						
						float[] detailCoefficients = new float[U];
						HashMap<Integer, Float> temp;
						
						float detailCo;
						float avgCo;
							for (int i = 0; i < numLevels; i++) {
								System.out.println("layer i = "+i);
								temp = new HashMap<Integer, Float>();
								int baseInd = (int)(U/Math.pow(2, i+1) + 1);
								
								for (int j = 0; j < U/(Math.pow(2, i)); j+=2) {
									int ind = baseInd + j/2;
									if (histo[j] != 0 || histo[j+1] != 0) {
										detailCo = (histo[j+1] - histo[j])/2;
										avgCo = (histo[j+1] + histo[j])/2;
										detailCoefficients[ind-1] = detailCo;
										temp.put(j/2, avgCo);
									}	
								}
								for (int ind = 0; ind < histo.length; ind++)
									if (temp.containsKey(ind))
										histo[ind] = temp.get(ind);
									else
										histo[ind] = 0;
								temp.clear();
							}
					
						
						//selecting top k
						PriorityQueue<IntDouble2> pq = new PriorityQueue<IntDouble2>(U);
						pq.add(new IntDouble2(1, (double)histo[0]));
						histo = null;
						
						for (int i = 1; i < U; i++) {
							if (detailCoefficients[i] != 0)
								pq.add(new IntDouble2(i+1, (double)detailCoefficients[i]));
						}
						detailCoefficients = null;
							
						StringBuilder sb = new StringBuilder(keyOut+"_");
						for (int i = 0; i < k; i++) {
							IntDouble2 val = pq.poll();
							if (val == null)
								break;
							if (val.f1 >= 0) {
//								out.collect(new Tuple4<String, Integer, Integer, Float>(keyOut, 0, val.f0, val.f1.floatValue()));
//								sb.append(keyOut + ",0," + val.f0 + "," + val.f1 + ";");
								sb.append("0," + val.f0 + "," + val.f1 + ";");
								break;
							}
//							out.collect(new Tuple4<String, Integer, Integer, Float>(keyOut, -1, val.f0, val.f1.floatValue()));
//							sb.append(keyOut + ",-1," + val.f0 + "," + val.f1 + ";");
							sb.append("-1," + val.f0 + "," + val.f1 + ";");
							
						}
						for (int i = pq.size(); i > k; i--) {
							IntDouble2 val = pq.poll();
							if (val == null)
								break;
//							out.collect(new Tuple4<String, Integer, Integer, Float>(keyOut, 0, val.f0, val.f1.floatValue()));
//							sb.append(keyOut + ",0," + val.f0 + "," + val.f1 + ";");
							sb.append("0," + val.f0 + "," + val.f1 + ";");
						}
						
						while (!pq.isEmpty()) {
							IntDouble2 val = pq.poll();
							if (val.f1 <= 0) {
//								out.collect(new Tuple4<String, Integer, Integer, Float>(keyOut, 0, val.f0, val.f1.floatValue()));
//								sb.append(keyOut + ",0," + val.f0 + "," + val.f1 + ";");
								sb.append("0," + val.f0 + "," + val.f1 + ";");
								continue;
							}
//							out.collect(new Tuple4<String, Integer, Integer, Float>(keyOut, 1, val.f0, val.f1.floatValue()));
//							sb.append(keyOut + ",1," + val.f0 + "," + val.f1 + ";");
							sb.append("1," + val.f0 + "," + val.f1 + ";");
							
						}
						out.collect(sb.toString());
					}
				})
				;
		
		
		/**
		 * Phase 1 - reduce: compute 
		 */
		DataSet<Row> bound = phase1.flatMap(new FlatMapFunction<String, Entry>() {
			private static final long serialVersionUID = 3316854148689534914L;

			@Override
			public void flatMap(String value, Collector<Entry> out) throws Exception {
				String[] tokens = value.split("_");
				String keyOut = tokens[0];
				String[] tuples = tokens[1].split(";");
				List<IntDouble2> result = new ArrayList<IntDouble2>();
				for (String tupleStr : tuples) {
					String[] tmp = tupleStr.split(",");
					int f2 = Integer.valueOf(tmp[1]);
					float f3 = Float.valueOf(tmp[2]);
					int f1 = Integer.valueOf(tmp[0]);
					
					//check if this tmp represents a key-coef that belong to top k, either positive or negative.
					if (f1 < 0 || f1 > 0)
						result.add(new IntDouble2(f2, (double)f3));
				}
				out.collect(new Entry(keyOut, result));
			}
		})
		.reduceGroup(new Phase1Reducer(k))	
		;

		//Phase 2, recompute table R (see histogram slide for table R)
		DataSet<Row> phase2 = phase1
		.map(new Phase2Mapper_2(k))
		.withBroadcastSet(bound, "bounds")
		.reduceGroup(new Phase2Reducer(k))
		.withBroadcastSet(bound, "bounds")
		;
		
		
		//Phase 3: compute final top K
		DataSet<IntDouble> phase3 = 
		phase1
		.map(new Phase3Mapper_2())
		.withBroadcastSet(phase2, "bounds2")
		.reduceGroup(new Phase3Reducer(k))
		.withBroadcastSet(phase2, "bounds2");

		phase3.print();
		
	}
}
