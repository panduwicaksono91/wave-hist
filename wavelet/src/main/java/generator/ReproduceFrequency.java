package main.java.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

/**
 * @author Shibo Cheng
 * rebuild the frequency with coefficients.
 */
public class ReproduceFrequency {
    /**
     * rebuild the frequency with coefficients., write to outputfile path. For calculating SSE later
     *
     * @param args inputfile path, outputfile path
     */
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        // domain size is fixed to 2^29
        int U = (int) Math.pow(2, 29);
        File file = new File(inputFile);
        BufferedReader reader = null;
        HashMap<Integer, Float> wavelet = new HashMap<Integer, Float>();
        float[] avgs = new float[U * 2 + 1];
        int level = 0;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                String[] substring = tempString.replace("(", "").replace(")", "").split("[\\(\\)\\s+,]");
                wavelet.put(Integer.valueOf(substring[0]), Float.valueOf(substring[1]));
            }
            reader.close();
            System.out.println("finish reading");
            level = (int) (Math.log(U) / Math.log(2));
            //avgs start from 1, so set 0 as a dummy value
            avgs[0] = 0;
            //based on algorithm, coefficient[1] is an average
            if (wavelet.containsKey(1)) {
                avgs[1] = wavelet.get(1);
                avgs[2] = wavelet.get(1);

            } else {
                avgs[1] = 0;
                avgs[2] = 0;
            }
            //rebuild avgs based on coef and previous level of avg. the final level avg is frequency
            float coJ = 0;
            for (int i = 1; i <= level; i++) {
                System.out.println("level:" + i);
                for (int j = (int) Math.pow(2, i - 1) + 1; j <= (int) Math.pow(2, i); j++) {

                    if (wavelet.containsKey(j)) {
                        coJ = wavelet.get(j);
                    } else {
                        coJ = 0;
                    }
                    avgs[j * 2 - 1] = avgs[j] - coJ;
                    avgs[j * 2] = avgs[j] + coJ;
                }
            }
            //write frequency file
            BufferedWriter bufferWriter = new BufferedWriter(new java.io.FileWriter( new File(outputFile)));
            for (int i = 1; i <= U; i++) {
                bufferWriter.write(i + "," + Math.round(avgs[U + i]) + "\n");
            }
            bufferWriter.flush();
            bufferWriter.close();
        } catch (
                Exception e)

        {
            e.printStackTrace();
        }
    }

}
