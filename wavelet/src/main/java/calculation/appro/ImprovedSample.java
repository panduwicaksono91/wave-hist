package main.java.calculation.appro;

import main.java.calculation.exact.sendcoef.IntFloat;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;


/**
 * @author Shibo Cheng
 * Improved Sampling class,removed items with small frequency after first round sampling, based on basic-s
 */
public class ImprovedSample {
    /**
     * execute improved-s, generate a wavelet tree and write to disk.
     *
     * @param args inputfile path,  k: the number of coefficient to output,, ee:ε in the paper parameter for sample size, outputfile path
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        // domain size is fixed to 2^29
        int U = (int) Math.pow(2, 29);
        //number of Levels of the wavelet tree
        int numLevels = (int) (Math.log(U) / Math.log(2));
        int k = Integer.valueOf(args[1]);
        double ee = Double.valueOf(args[2]);
        String outputFile = String.valueOf(args[3]);
        //number of records in the dataset
        int n = 1350000000;
        //probability of a record being selected
        final double pp = 1 / (ee * ee * n);
        //1/pp, the interval for reading data with item selected probability=pp.
        int jumpstep = (int) Math.round(ee * ee * n);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> sample = env.readTextFile(inputFile)
                //sampling data in partition level, based on ee and number of records in this split
                .mapPartition(new MapPartitionFunction<String, Tuple2<Integer, Integer>>() {
                    public void mapPartition(Iterable<String> values, Collector<Tuple2<Integer, Integer>> out) throws IOException {

                        // store sampled item frequency
                        HashMap<Integer, Integer> freqs = new HashMap<Integer, Integer>();
                        //tj is number of sampled records in this split
                        int tj = 0;
                        for (String s : values) {
                            // normalize and split the line
                            String[] tokens = s.split("\\W+|,");
                            //select records by jumpstep
                            for (int i = 0; i < tokens.length; i += jumpstep) {
                                String token = tokens[i];
                                int key = Integer.valueOf(token) - 1;
                                if (freqs.containsKey(key)) {
                                    freqs.put(key, freqs.get(key) + 1);
                                } else {
                                    freqs.put(key, 1);
                                }
                                tj++;
                            }
                        }
                        //For analysis, calculate number of items that passed the threshold and write to disk.
                        int numberPassThreshold = 0;
                        for (int i = 0; i < U; i++) {
                            //remove item with frequency smaller than ee*tj
                            if (freqs.containsKey(i) && freqs.get(i) >= ee * tj) {
                                numberPassThreshold++;
                                out.collect(new Tuple2<Integer, Integer>(i + 1, freqs.get(i)));
                            }
                        }
                        BufferedWriter bufferWriter = new BufferedWriter(new java.io.FileWriter(new File("/share/flink/tmp/thresholdImprove.txt")));
                        bufferWriter.write("Sampled data in this partition/threshold/passed the threshold: " + tj+ ";" + ee*tj+ ";" + numberPassThreshold);
                        bufferWriter.flush();
                        bufferWriter.close();
                    }

                });

        DataSet<Tuple2<Integer, Integer>> s2freqs = sample.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                int key = 0;
                int value = 0;
                //caculate item frequency
                for (Tuple2<Integer, Integer> t : iterable) {
                    key = t.f0;
                    value += t.f1;
                }
                //   divided by pp, to get unbiased estimator
                collector.collect(Tuple2.of(key, (int) Math.round(value / pp)));
            }
        });
        DataSet<IntFloat> coefs = s2freqs.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, IntFloat>() {

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
        try

        {
            coefs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
        } catch (
                Exception e)

        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
