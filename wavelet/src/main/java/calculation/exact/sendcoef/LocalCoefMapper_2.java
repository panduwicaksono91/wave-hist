package main.java.calculation.exact.sendcoef;

import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
/**
 * 
 * @author dieutth
 * FlatMap to compute local wavelet tree, using array (in flatMap)
 */
public class LocalCoefMapper_2 implements FlatMapFunction<String, IntFloat>{
	
	private static final long serialVersionUID = -3549184163020471119L;
	
	private int U;
	private int numLevels;
	
	public LocalCoefMapper_2(int U, int numLevels) {
		this.U = U;
		this.numLevels = numLevels;
	}

	@Override
	public void flatMap(String arg0, Collector<IntFloat> arg1) throws Exception {
		//histo as an array, first to store local coef; then to store avg-coef from previous level when 
		// we need to compute detail coef at current level
		float[] histo = new float[U];
       
		//compute local frequency
		for (String s : arg0.split(",")) {
			int key = Integer.valueOf(s)-1;
			histo[key] += 1;
		}
		
        float[] detailCoefficients = new float[U];
        HashMap<Integer, Float> temp;
        float detailCo;
        float avgCo;
        
        //compute wavelet-tree, bottom up
        for (int i = 0; i < numLevels; i++) {
            temp = new HashMap<Integer, Float>();
            int baseInd = (int) (U / Math.pow(2, i + 1) + 1);

            for (int j = 0; j < U / (Math.pow(2, i)); j += 2) {
                int ind = baseInd + j / 2;
                if (histo[j] != 0 || histo[j + 1] != 0) {
                    detailCo = (histo[j + 1] - histo[j]) / 2;
                    avgCo = (histo[j + 1] + histo[j]) / 2;
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
        }
        
        //Coef with index 1 is an avg-coef, so it is stored in histo[0]
        arg1.collect((new IntFloat(1, histo[0])));
        //add other detail coef
        for (int i = 1; i < U; i++) {
            if (detailCoefficients[i] != 0)
                arg1.collect(new IntFloat(i + 1, detailCoefficients[i]));
        }
	}
}
