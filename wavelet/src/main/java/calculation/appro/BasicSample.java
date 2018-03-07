package main.java.calculation.appro;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import main.java.calculation.exact.sendcoef.IntFloat;
import main.java.calculation.exact.sendv.ComputeWaveletGroupReduce;

/**
 * @author Shibo Cheng
 * Basic Sampling class, sampling first before sending all item frequency to the reducer.
 */
public class BasicSample {
    /**
     * execute basic-s, generate a wavelet tree and write to disk.
     *
     * @param args inputfile path,  k: the number of coefficient to output, ee:ε in the paper  parameter for sample size, outputfile path
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
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                                 // normalize and split the line
                                 String[] tokens = value.split("\\W+|,");
                                 //select records by jumpstep
                                 for (int i = 0; i < tokens.length; i += jumpstep) {
                                     String token = tokens[i];
                                     out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(token), 1));
                                 }
                             }
                         }
                );

        DataSet<IntFloat> freqs = sample.groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
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
                })
                .reduceGroup(new ComputeWaveletGroupReduce(k, U, numLevels));
        try {

            freqs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
