package main.java.calculation.appro;

import main.java.calculation.exact.sendcoef.IntFloat;
import main.java.calculation.exact.sendv.ComputeWaveletGroupReduce;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/**
 * @author Shibo Cheng
 * For analyzing intermediate result after twolevel sampling, includes total number of records in this partition, total number of itemid in this partiton,
 * number of sampled records in this partition, number of sampled itemid, threshold, how many item passed the 1st level sampling, how many item
 * passed the 2nd sampling
 */
public class TwoLevelSampleAnalysis {
    /**
     * execute twolevel-s analysis, generate intermediate result and write to disk.
     *
     * @param args inputfile path, k: the number of coefficient to output,m: the number of parallelism,
     *             ee: parameter for sample size (epsilon in the paper)a
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
                        HashMap<Integer, Integer> freqsAll = new HashMap<Integer, Integer>();

                        //For analysis, tj is number of sampled records in this split,
                        int tj = 0;
                        int tjAll = 0;
                        for (String s : values) {
                            // normalize and split the line
                            String[] tokens = s.split("\\W+|,");
                            tjAll += tokens.length;

                            for (int i = 0; i < tokens.length; i++) {
                                String token = tokens[i];
                                int key = Integer.valueOf(token) - 1;
                                if (freqsAll.containsKey(key)) {
                                    freqsAll.put(key, freqsAll.get(key) + 1);
                                } else {
                                    freqsAll.put(key, 1);
                                }

                                if (i % jumpstep == 0) {
                                    if (freqs.containsKey(key)) {
                                        freqs.put(key, freqs.get(key) + 1);
                                    } else {
                                        freqs.put(key, 1);
                                    }
                                    tj++;
                                }
                            }
                        }
                        //For analysis, calculate number of items that passed the threshold and write to disk.
                        int numberPassThreshold = 0;
                        int numberPass2nd = 0;
                        int numberSampledItem = 0;

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
                                else if (random.nextDouble() <= em * temp) {
                                    numberPass2nd++;
                                    out.collect(new Tuple2<Integer, Integer>(i + 1, 0));
                                }

                            }
                        }
                        BufferedWriter bufferWriter = new BufferedWriter(new java.io.FileWriter("/share/flink/tmp/thresholdTwoLevel2.txt", true));
                        bufferWriter.write("total records/total itemid/Sampled records /sampled itemid/threshold/passed the threshold/passed 2nd: " + ";" + tjAll + ";" + freqsAll.size() + ";" + tj + ";" + numberSampledItem + ";" + 1 / em + ";" + numberPassThreshold + ";" + numberPass2nd);
                        bufferWriter.newLine();

                        bufferWriter.flush();
                        bufferWriter.close();
                    }

                });
        sample.writeAsCsv("/share/flink/tmp/TwoLevelMappartition2.csv").setParallelism(1);
        env.execute();

    }
}
