package main.java.calculation.appro;

import main.java.calculation.exact1.IntDouble;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Random;

public class ImprovedSample {
    // filepath, k,ee;
    public static void main(String[] args) throws Exception {
        //     String inputFile = "wave-hist\\wavelet\\src\\resource\\toydataset_1.txt";
        String inputFile = args[0];
//        int U = (int) Math.pow(2, 29);
        int U = (int) Math.pow(2, 3);
        //number of Levels of the wavelet tree
        int numLevels = (int) (Math.log(U) / Math.log(2));
        //      int k = 3;
        int k = Integer.valueOf(args[1]);

        //       double ee = 0.0001;
        double ee = Double.valueOf(args[2]);
        System.out.println(ee);
        int n = 1350000000;
        // final double pp = 1 / (ee * ee * n);
        double pp = 0.5;

        System.out.println(pp);
        Random random = new Random();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //	ReservoirSamplerWithoutReplacement sampler=new ReservoirSamplerWithoutReplacement<Integer>(10);
        DataSet<Tuple2<Integer, Integer>> sample = env.readTextFile(inputFile)
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                                 // normalize and split the line
                                 String[] tokens = value.split("\\W+|,");
                                 int tj = tokens.length;
                                 int[] freqs = new int[U];
                                 for (String token : tokens) {
                                     if (token.length() > 0 && random.nextDouble() <= pp) {
                                         int key = Integer.valueOf(token) - 1;
                                         freqs[key] += 1;

                                         //out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(token), 1));
                                     }
                                 }
                                 for (int i = 0; i < U; i++) {
                                     if (freqs[i] >= ee * tj)
                                         out.collect(new Tuple2<Integer, Integer>(i + 1, freqs[i]));
                                 }
                             }
                         }
                );
        //       sample.print();
//        DataSet<Tuple2<Integer, Integer>> s1freqs = sample.groupBy(0)
//                .sum(1);
//        s1freqs.print();
//        DataSet<Tuple2<Integer, Integer>> s2freqs = s1freqs.flatMap
//                (new FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//                     @Override
//                     public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
//                         if (value.f1 >= 1 / em) {
//                             out.collect(value);
//                         } else {
//                             if (random.nextDouble() <= em * value.f1)
//                                 out.collect(Tuple2.of(value.f0, 0));
//                         }
//                     }
//                 }
//                )
        DataSet<Tuple2<Integer, Integer>> s2freqs = sample.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                int key = 0;
                int value = 0;
                for (Tuple2<Integer, Integer> t : iterable) {
                    key = t.f0;
                    value += t.f1;
                }
                //   divided by pp, to get unbiased estimator

                collector.collect(Tuple2.of(key, (int) Math.round(value / pp)));
            }
        });
        //     s2freqs.print();
        DataSet<IntDouble> coefs = s2freqs.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, IntDouble>() {

            @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> arg0, Collector<IntDouble> arg1)
                    throws Exception {

                System.out.println("starting calculation");
                double[] histo = new double[U];
                System.out.println(histo.length);
                for (Tuple2<Integer, Integer> t : arg0) {
                    histo[t.f0 - 1] = t.f1;
                }
                System.out.println("finish histo numbers putting");

//											HashMap<Integer, Double> detailCoefficients = new HashMap<Integer, Double>(U);
                double[] detailCoefficients = new double[U];
                HashMap<Integer, Double> temp;

                double detailCo;
                double avgCo;
                for (int i = 0; i < numLevels; i++) {
                    System.out.println("layer i = " + i);
                    temp = new HashMap<Integer, Double>();
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
                PriorityQueue<IntDouble> pq = new PriorityQueue<IntDouble>(U);
                pq.add(new IntDouble(1, histo[0]));
                histo = null;

                for (int i = 0; i < U; i++)
                    if (detailCoefficients[i] != 0)
                        pq.add(new IntDouble(i + 1, detailCoefficients[i]));
                detailCoefficients = null;
//											it = detailCoefficients.entrySet().iterator();
//											while (it.hasNext()) {
//												Entry<Integer, Double> item = it.next();
//												pq.add(new IntDouble(item.getKey(), item.getValue()));
//												it.remove();
//											}

                for (int i = 0; i < k; i++) {
                    IntDouble id = pq.poll();
                    arg1.collect(id);
                    System.out.println(id);
                }


            }
        });
        try {
            coefs.print();
            String classname=Thread.currentThread().getStackTrace()[1].getClassName();
            coefs.writeAsText(classname.substring(classname.lastIndexOf(".")+1)+"Coefficients.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
