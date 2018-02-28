package main.java.calculation.exact.sendcoef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

public class BaselineImpl {
	public static void buildWaveletWithoutFlink() {
		Double[] freq = new Double[] {(double) 3, (double) 5, (double) 10, (double) 8, (double) 2, (double) 2, (double) 10, (double) 14};
		int numLevels = (int)(Math.log(freq.length)/Math.log(2));
		
		List<Double> avgCoefficients = new ArrayList<Double>(Arrays.asList(freq));
		
		List<Double> detailCoefficients = new ArrayList<Double>();
		List<Double> temp;

		for (int i = 0; i < numLevels; i++) {
			temp = new ArrayList<Double>();
			for (int j = 0; j < avgCoefficients.size(); j+=2) {
				double detailCo = (avgCoefficients.get(j+1) - avgCoefficients.get(j))/2;
				double avgCo = (avgCoefficients.get(j+1) + avgCoefficients.get(j))/2;
				detailCoefficients.add(detailCo);
				temp.add(avgCo);
			}
			avgCoefficients = temp;
		}
		
		for (Double i : detailCoefficients)
			System.out.println(i);
	}
	//input k output
	public static void main(String[] args) throws Exception {
		//file content: "7,8,1,8,1,8,7,8,3,2,7,7,3,4,7,2,3,4,8,7,4,4,2,6,8,8,8,4,4,8,4,8,3,5,6,8,7,3,4,3,8,8,2,2,3,1,3,7,3,3,7,7,5,8"
		//this produce the histogram in domain u=8 as described in the paper (figure 1)
		
//		String inputFile = "src\\resource\\toydataset_1.txt";
		String inputFile = args[0];

		//domain U
		int U = (int)Math.pow(2,29);
		//number of Levels of the wavelet tree
		int numLevels = (int)(Math.log(U)/Math.log(2));
		//desired number of coefficients to keep
		int k = Integer.valueOf(args[1]);
		String outputFile=args[2];
	
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<IntFloat> freqs = env.readTextFile(inputFile)
								  .flatMap(new LocalIndexCoefficientsFlatMapper(U, numLevels))
								  .groupBy(0) //group by index i of the wavelet tree
								  .sum(1)
								  .mapPartition(new TopKMapPartition(k))
								  .reduceGroup(new TopKReducer(k))
								;
		freqs.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		env.execute();
	}
}
