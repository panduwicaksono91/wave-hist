package main.java.calculation.exact.sendcoef;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

/**
 * 
 * @author dieutth
 * Compute wavelet tree with SendCoef algorithm: first, build local wavelet tree.
 * Then sum up local trees based on index to build global wavelet tree
 *
 */
public class SendCoef {
	/**
	 * 
	 * @param args: 0)inputFile path, 1)numLevels, 2)k: number of coefficient to output,
	 *  3)mapperOption,  4)outputFile path
	 */
    public static void main(String[] args) {
    	
        String inputFile = args[0];
        
        //number of Levels of the wavelet tree
        int numLevels = Integer.valueOf(args[1]);
        
        // domain size
        int U = (int) Math.pow(2, numLevels);
        
        int k = Integer.valueOf(args[2]);
        String outputFile = String.valueOf(args[4]);
        
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		String mapperOption = args[3];
		FlatMapFunction<String, IntFloat> mapper = null;
		
		//mapperOption: 1 if compute local wavelet tree using HashMap, 2 if using array
		if (mapperOption == "1")
			mapper = new LocalIndexCoefficientsFlatMapper(U, numLevels);
		else
			mapper = new LocalCoefMapper_2(U, numLevels);

		DataSet<IntFloat> freqs = env.readTextFile(inputFile)
								  .flatMap(mapper) //building local wavetlet tree
								  .groupBy(0) //group by index i of the wavelet tree
								  .sum(1)
								  .mapPartition(new TopKMapPartition(k))
								  .reduceGroup(new TopKReducer(k))
								;
		
		try {
			freqs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
