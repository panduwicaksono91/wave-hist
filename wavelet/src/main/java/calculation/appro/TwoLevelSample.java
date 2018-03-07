package main.java.calculation.appro;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import main.java.calculation.exact.sendcoef.IntFloat;
import main.java.calculation.exact.sendv.ComputeWaveletGroupReduce;

/**
 * @author Shibo Cheng
 * Two-Level Sampling class,removed items with small frequency after first round sampling,
 * survive part of those items in second round sampling
 */
public class TwoLevelSample {
    /**
     * execute twolevel-s, generate a wavelet tree and write to disk.
     *
     * @param args inputfile path, k: the number of coefficient to output,m: the number of parallelism,
     *  ee: parameter for sample size (epsilon in the paper)a, outputfile path
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        // domain size is fixed to 2^29
        int U = (int) Math.pow(2, 29);
        //number of Levels of the wavelet tree
        int numLevels = (int) (Math.log(U) / Math.log(2));
        //the number of coefficient to output
        int k = Integer.valueOf(args[1]);
        int mapper = Integer.valueOf(args[2]);

        double ee = Double.valueOf(args[3]);
        //1/em is the threshold for removing item with small frequency
        double em = Math.sqrt(mapper) * ee;
        String outputFile = String.valueOf(args[4]);
        //number of records in the dataset
        int n = 1350000000;
        //probability of a record being selected
        final double pp = 1 / (ee * ee * n);
        //1/pp, the interval for reading data with item selected probability=pp.
        int jumpstep = (int) Math.round(ee * ee * n);

        Random random = new Random();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> sample = env.readTextFile(inputFile)
                //sampling data in partition level, based on ee and number of records in this split
                .mapPartition(new MapPartitionFunction<String, Tuple2<Integer, Integer>>() {
                    public void mapPartition(Iterable<String> values, Collector<Tuple2<Integer, Integer>> out) throws IOException {
                        // store sampled item frequency
                        HashMap<Integer, Integer> freqs = new HashMap<Integer, Integer>();
                        //For analysis, tj is number of sampled records in this split,
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
                            //For analysis, calculate number of items that passed the threshold and write to disk.
                            int numberPassThreshold = 0;
                            int numberSampledItem=0;

                            for (int i = 0; i < U; i++) {
                                if (freqs.containsKey(i)) {
                                    numberSampledItem++;
                                    int temp = freqs.get(i);
                                    //select item with frequency not smaller than 1/em,
                                    if (temp >= 1 / em) {
                                        numberPassThreshold++;
                                        out.collect(new Tuple2<Integer, Integer>(i + 1, temp));
                                    }
                                    //survive them with a probability propotional to their frequency
                                    else if (random.nextDouble() <= em * temp)
                                        out.collect(new Tuple2<Integer, Integer>(i + 1, 0));

                                }
                            }
                            BufferedWriter bufferWriter = new BufferedWriter(new java.io.FileWriter(new File("/share/flink/tmp/thresholdTwoLevel.txt")));
                            bufferWriter.write("Sampled data in this partition/no.sampled item/threshold/passed the threshold: " + tj + ";"+numberSampledItem+ ";"  + 1/em+";" + numberPassThreshold);
                            bufferWriter.flush();
                            bufferWriter.close();
                        }
                    }
                });

        DataSet<Tuple2<Integer, Integer>> s2freqs = sample.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                int key = 0;
                int value = 0;
                int m = 0;
                for (Tuple2<Integer, Integer> t : iterable) {
                    key = t.f0;
                    if (t.f1 > 0) {
                        value += t.f1;
                        //for items that survive in second level sampling
                    } else {
                        m++;
                    }
                }
                //   divided by pp, to get unbiased estimator
                value = (int) ((value + m / em) / pp);
                collector.collect(Tuple2.of(key, value));
            }
        });
        DataSet<IntFloat> coefs = s2freqs.reduceGroup(new ComputeWaveletGroupReduce(k, U, numLevels));
        try {
            coefs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
