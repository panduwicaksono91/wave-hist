package main.java.calculation.exact.hwtopk;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class TestListDataset {
	 public static void main(String[] args) throws Exception {
		 ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		 
		 int U = 8, numLevels = 3;
		 int k = 3;
			String[] paths = new String[] {"D:\\data\\mapper_7b2c1cb652e078ae892a0bc92de4c19dffb85f0b", 
					"D:\\data\\mapper_c6f8ab7cbba9b06de294237408dbff58631e3777"};
//			List<DataSet<IntDouble2>> mappers = new ArrayList<DataSet<IntDouble2>>();
			
			for (String filePath : paths) {
				
				DataSet<Entry> mapper = env.readTextFile(filePath)
						.map(new Phase2Mapper(filePath, k))
						;
//				mappers.add(mapper);
			}
			
			System.out.println("Mapper");
//			for (DataSet<IntDouble2> mapper : mappers) {
//				System.out.println("----------");
//				mapper.print();
//			}

	}

}
