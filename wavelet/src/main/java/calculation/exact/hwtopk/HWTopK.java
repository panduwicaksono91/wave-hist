package main.java.calculation.exact.improved;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import main.java.calculation.exact1.IntDouble;


public class HWTopK {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//Parameters for testing in cluster
//		String inputFile = args[0];
//		int U = (int)Math.pow(2, 29);
//		int numLevels = (int)(Math.log(U)/Math.log(2));
//		int k = 30;
//		String folder = "/share/flink/tmp/mapper/";
		
		//Parameters for testing in local
		String inputFile = "D:\\gitworkspace\\wave-hist\\wavelet\\src\\resource\\toydataset_1.txt";
		int U = 8;
		int numLevels = (int)(Math.log(U)/Math.log(2));
		int k = 3;
		String folder = "D://data//mappers//";
		
		DataSet<Row> phase1 = env.readTextFile(inputFile)
				.map(new Phase1Mapper(U, numLevels, k, folder))
				.reduceGroup(new Phase1Reducer(k))
				;
		//clumsy way to force phase 1 to execute 
//		phase1.count();
		DataSet<Row> phase2 = env.readTextFile(folder)
							.map(new Phase2Mapper(k))
							.withBroadcastSet(phase1, "bounds")
							.reduceGroup(new Phase2Reducer(k))
							.withBroadcastSet(phase1, "bounds")
							;
		
		DataSet<IntDouble> phase3 = 
				env.readTextFile(folder)
				.map(new Phase3Mapper())
				.withBroadcastSet(phase2, "bounds2")
				.reduceGroup(new Phase3Reducer(k))
				.withBroadcastSet(phase2, "bounds2");
				
		phase3.print();
		
	}
}
