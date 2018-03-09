package test.java;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

import main.java.calculation.exact.sendcoef.IntFloat;
import main.java.calculation.exact.sendcoef.LocalIndexCoefficientsFlatMapper;
import main.java.calculation.exact.sendcoef.TopKMapPartition;
import main.java.calculation.exact.sendcoef.TopKReducer;
import main.java.calculation.exact.sendv.ComputeWaveletGroupReduce;
/**
 * 
 * @author dieutth
 * Test correctness of mapper and reducer.
 */
public class TestWavelet{
	
	/**
	 * Test method reduce of ComputeWaveletGroupReducer. ComputeWaveletGroupReducer is used to build the final 
	 * wavelet tree and then select top k from it. Its input is the global frequency of each key in the dataset (computed
	 * from mapping phase).
	 * We test with the dummy key-frequency pairs (ref from the paper).
	 * {1:3, 2:5, 3:10, 4:8, 5:2, 6:2, 7:10, 8:14} 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testComputeWaveletGroupReduce() throws Exception {
		int U = 8, numLevels = 3, k = 7; 
		ComputeWaveletGroupReduce gr = new ComputeWaveletGroupReduce(k, U, numLevels);
		
		int[] domain = new int[8];
		for (int i = 1; i <= domain.length; i++)
			domain[i-1] = i;
		
		int[] freq = new int[] {3, 5, 10, 8, 2, 2, 10, 14};
		
		List<Tuple2<Integer,Integer>> tuples = new ArrayList<Tuple2<Integer,Integer>>();
		for (int i = 0; i < freq.length; i++) {
			tuples.add(Tuple2.of(domain[i], freq[i]));
		}
		Iterable<Tuple2<Integer,Integer>> in = tuples;
		
		List<IntFloat> actualResult = new ArrayList<IntFloat>();
		Collector<IntFloat> out = new ListCollector<IntFloat>(actualResult); 
		
		gr.reduce(in, out);
		
		/* Expected wavele tree, result is not rounded up as in the paper.*/
		List<IntFloat> expectedResult =  Arrays.asList(new IntFloat[]{
					new IntFloat(1, 6.75f),
					new IntFloat(2, 0.25f),
					new IntFloat(3, 2.5f),
					new IntFloat(4, 5f),
					new IntFloat(5, 1f),
					new IntFloat(6, -1f),
					new IntFloat(8, 2f),
		});
		
		
		if (!expectedResult.containsAll(actualResult))
			fail("Top k coefficients don't contain all elements it should contain.");
		
		if (!actualResult.containsAll(expectedResult))
			fail("Top k coefficients contains some elements it shouldn't contain.");
	}
	
	
	/**
	 * Test SendCoef end-to-end
	 * @throws Exception 
	 */
	@Test
//	@Ignore
	public void testSendCoef() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		int U = 8, numLevels = 3, k = 4;
		
		List<IntFloat> actualResult = 
		  env.fromElements("7,8,1,8,1,8,7,8,3,2,7,7,3,4,7,2,3,4,8,7,4,4,2,6,8,8,8,4,4,8,4,8,3,5,6,8,7,3,4,3,8,8,2,2,3,1,3,7,3,3,7,7,5,8")
		  .flatMap(new LocalIndexCoefficientsFlatMapper(U, numLevels)) //building local wavetlet tree
		  .groupBy(0) //group by index i of the wavelet tree
		  .sum(1)
		  .mapPartition(new TopKMapPartition(k))
		  .reduceGroup(new TopKReducer(k))
		  .collect()
		  ;
		
		List<IntFloat> expectedResult =  Arrays.asList(new IntFloat[]{
				new IntFloat(1, 6.75f),
				new IntFloat(3, 2.5f),
				new IntFloat(4, 5f),
				new IntFloat(8, 2f),
		
		});
		
		if (!expectedResult.containsAll(actualResult))
			fail("Top k coefficients don't contain all elements it should contain.");
		
		if (!actualResult.containsAll(expectedResult))
			fail("Top k coefficients contains some elements it shouldn't contain.");
	}
	
}
