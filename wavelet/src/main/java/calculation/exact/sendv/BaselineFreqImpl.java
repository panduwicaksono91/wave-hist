package main.java.calculation.exact.sendv;


import java.util.HashMap;
import java.util.PriorityQueue;

import main.java.calculation.exact.sendcoef.IntFloat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import main.java.calculation.exact.sendcoef.IntDouble;

public class BaselineFreqImpl {
    public static void main(String[] args) {
        //Parameters for testing in cluster
//		String inputFile = args[0];
//		int U = (int)Math.pow(2, 29);
//		int numLevels = (int)(Math.log(U)/Math.log(2));
//		int k = 30;

        //Parameters for testing in local
//		String inputFile = "D:\\gitworkspace\\wave-hist\\wavelet\\src\\resource\\toydataset_1.txt";
        String inputFile = args[0];
        int U = (int) Math.pow(2, 29);
        int k = Integer.valueOf(args[1]);
        String outputFile = args[2];
//		int U = 8;
        int numLevels = (int) (Math.log(U) / Math.log(2));
//		int k = 8;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<IntFloat> freqs = env.readTextFile(inputFile)
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
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

                )
                .groupBy(0)
                .sum(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, IntFloat>() {

                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Integer>> arg0, Collector<IntFloat> arg1)
                            throws Exception {

                        System.out.println("starting calculation");
                        float[] histo = new float[U];
                        System.out.println(histo.length);
                        for (Tuple2<Integer, Integer> t : arg0) {
                            histo[t.f0 - 1] = t.f1;
                        }
                        System.out.println("finish histo numbers putting");

//											HashMap<Integer, Double> detailCoefficients = new HashMap<Integer, Double>(U);
                        float[] detailCoefficients = new float[U];
                        HashMap<Integer, Float> temp;

                        float detailCo;
                        float avgCo;
                        for (int i = 0; i < numLevels; i++) {
                            System.out.println("layer i = " + i);
                            temp = new HashMap<Integer, Float>();
                            int baseInd = (int) (U / Math.pow(2, i + 1) + 1);

                            for (int j = 0; j < U / (Math.pow(2, i)); j += 2) {
                                int ind = baseInd + j / 2;
                                if (histo[j] != 0 || histo[j + 1] != 0) {
                                    detailCo = (histo[j + 1] - histo[j]) / 2;
                                    avgCo = (histo[j + 1] + histo[j]) / 2;
//															detailCoefficients.put(ind, detailCo);
                                    detailCoefficients[ind - 1] = detailCo;
                                    temp.put(j / 2, avgCo);
                                }
                            }
                            for (int ind = 0; ind < histo.length; ind++)
                                if (temp.containsKey(ind))
                                    histo[ind] = temp.get(ind);
                                else
                                    histo[ind] = 0;
                            temp.clear();
//													it = temp.entrySet().iterator();
//													while (it.hasNext()) {
//														Entry<Integer, Double> item = it.next();
//														histo[item.getKey()] = item.getValue();
//														it.remove();
//													}
                        }

                        System.out.println("done with loading to histo. Starting queue");

                        //selecting top k
                        PriorityQueue<IntFloat> pq = new PriorityQueue<IntFloat>(U);
                        pq.add(new IntFloat(1, histo[0]));
                        histo = null;

                        for (int i = 1; i < U; i++) {
                            if (detailCoefficients[i] != 0)
                                pq.add(new IntFloat(i + 1, detailCoefficients[i]));
                        }
                        detailCoefficients = null;
//											it = detailCoefficients.entrySet().iterator();
//											while (it.hasNext()) {
//												Entry<Integer, Double> item = it.next();
//												pq.add(new IntDouble(item.getKey(), item.getValue()));
//												it.remove();
//											}

                        for (int i = 0; i < k; i++) {
                            IntFloat id = pq.poll();
                            arg1.collect(id);
                        }


                    }
                });


        try {
            freqs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
