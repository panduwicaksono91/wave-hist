package main.java.calculation.exact.sendv;

import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import main.java.calculation.exact.sendcoef.IntFloat;

/**
 * 
 * @author dieutth
 * A GroupReduce function to compute a complete wavelet tree on a domain U and select top k coef, based on a list of frequency.
 */
public class ComputeWaveletGroupReduce implements GroupReduceFunction<Tuple2<Integer, Integer>, IntFloat> {
	private static final long serialVersionUID = 8418290884644031488L;
	
	private int k;
	private int U;
	private int numLevels;
	/**
	 * 
	 * @param k: number of top coef to select after having a complete wavelet tree
	 * @param U: domain size 
	 * @param numLevels: number of levels of the wavelet tree. Can be derived from U by formula numLevels=log(U)/log(2)
	 */
	public ComputeWaveletGroupReduce(int k, int U, int numLevels) {
		this.k = k;
		this.U = U;
		this.numLevels = numLevels;
	}

    @Override
    public void reduce(Iterable<Tuple2<Integer, Integer>> arg0, Collector<IntFloat> arg1)
            throws Exception {
    	
    	//an array: at first, it stores freqs. Then, at each level when computing 
    	//detail coef, it stores avg-coef from previous level
        float[] histo = new float[U];
        
        //load frequency to histo array
        for (Tuple2<Integer, Integer> t : arg0) {
            histo[t.f0 - 1] = t.f1;
        }

        float[] detailCoefficients = new float[U];
        HashMap<Integer, Float> temp;
        float detailCo;
        float avgCo;

        //compute wavelet tree, bottom up, from level 0 to numLevels-1
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
            
            //update histo to compute detail coef in next level
            for (int ind = 0; ind < histo.length; ind++)
                if (temp.containsKey(ind))
                    histo[ind] = temp.get(ind);
                else
                    histo[ind] = 0;
            temp.clear();
        }

        
        PriorityQueue<IntFloat> pq = new PriorityQueue<IntFloat>(U);
        //The root of the wavelet tree is the avg value of all freq, stored previously in histo[0]
        pq.add(new IntFloat(1, histo[0]));
        histo = null;

        // Putting all non-zero detail coef into a PriorityQueue
        for (int i = 1; i < U; i++) {
            if (detailCoefficients[i] != 0)
                pq.add(new IntFloat(i + 1, detailCoefficients[i]));
        }
        detailCoefficients = null;

        // Selecting top k coef
        int count = k;
        while (count > 0 && !pq.isEmpty()) {
            arg1.collect(pq.poll());
            count--;
        }


    }
}
