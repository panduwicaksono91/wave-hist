package main.java.calculation.exact.sendv;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import main.java.calculation.exact.sendcoef.IntFloat;
/**
 * @author dieutth
 * 
 * Compute wavelet with the SendV algorithm.
 *
 */
public class BaselineFreqImpl {
	/**
	 * 
	 * @param args: 0)inputFile path, 1)numLevels, 2)k: number of coefficient to output, 3)outputFile path
	 */
    public static void main(String[] args) {
    	
        String inputFile = args[0];
        
        //number of Levels of the wavelet tree is fixed to 29
        int numLevels = Integer.valueOf(args[1]);
        
        // domain size
        int U = (int) Math.pow(2, numLevels);
        
        int k = Integer.valueOf(args[2]);
        String outputFile = String.valueOf(args[3]);
        
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<IntFloat> freqs = env.readTextFile(inputFile)
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
					private static final long serialVersionUID = -8587125551561224725L;

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
                .groupBy(0) //groupBy key (ie. record key, drawnn from domain size U)
                .sum(1)    //total frequency
                .reduceGroup(new ComputeWaveletGroupReduce(k, U, numLevels));
        

        try {
            freqs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
