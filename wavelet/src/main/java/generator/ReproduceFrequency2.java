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

//rebuild the frequency with coefficients and number of domain.
//generate a map of frequencies,<indexOfElement,frequency>
public class ReproduceFrequency2 {
    //inputfile outputfile
    public static void main(String[] args) throws Exception {
        //example file:(8,3) firstline, (numberOfDomain, numberOfCoefficient)
        // (3,2.5)  (indexOfCoefficentTree, coefficientValue)
        //(1,6.75)
        //(4,5.0)
        //2^29 =
        String inputFile = args[0];
        String outputFile = args[1];
        System.out.println("reproduce frequency2");

        System.out.println(inputFile);
        System.out.println(outputFile);
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        //	ReservoirSamplerWithoutReplacement sampler=new ReservoirSamplerWithoutReplacement<Integer>(10);
//        DataSet<Tuple2<Integer, Float>> sample = env.readTextFile(inputFile).flatMap(
//                new FlatMapFunction<String, Tuple2<Integer, Float>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<Integer, Float>> out) {
//                        // normalize and split the line
//                        String[] tokens = value.split("[\\(\\)\\s+,]");
//                        out.
//                    }
//                }
//        );
        int U = (int) Math.pow(2, 29);
//            int U = (int) Math.pow(2, 3);
        File file = new File(inputFile);
        BufferedReader reader = null;
        //       int numberOfDomain = 0;
        HashMap<Integer, Float> wavelet = new HashMap<Integer, Float>();
        float[] avgs = new float[U*2+1];
        //       HashMap<Integer,Double> freqs = new HashMap<Integer , Double>();
        int level = 0;
        try

        {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            //    int line = 1;
            while ((tempString = reader.readLine()) != null) {
                //        System.out.println("line " + line + ": " + tempString);
                String[] substring = tempString.replace("(", "").replace(")", "").split("[\\(\\)\\s+,]");
//                if (line == 1) {
//                    numberOfDomain = Integer.valueOf(substring[1]);
//                    numberOfK = Integer.valueOf(substring[2]);
//                } else {
                wavelet.put(Integer.valueOf(substring[0]), Float.valueOf(substring[1]));
//                }
                //     line++;
            }
            reader.close();
            System.out.println("finish reading");
            level = (int) (Math.log(U) / Math.log(2));
            avgs[0] = 0;
            if (wavelet.containsKey(1)) {
//                avgs.put(1, wavelet.get(1));
//                avgs.put(2, wavelet.get(1));
                avgs[1] = wavelet.get(1);
                avgs[2] = wavelet.get(1);

            } else {
                avgs[1] = 0;
                avgs[2] = 0;
            }

            float coJ = 0;
            for (int i = 1; i <= level; i++) {
                System.out.println("level:" + i);
                for (int j = (int) Math.pow(2, i - 1) + 1; j <= (int) Math.pow(2, i); j++) {

                    if (wavelet.containsKey(j)) {
                        coJ = wavelet.get(j);
                    } else {
                        coJ = 0;
                    }
//                    avgs.put(j*2-1,avgs.get(j)-coJ);
//                    avgs.put(j*2,avgs.get(j)+coJ);
//                    avgs.remove(j);
                    avgs[j * 2 - 1] = avgs[j] - coJ;
                    avgs[j * 2] = avgs[j] + coJ;
                }
            }
            //       System.out.println(avgs);
            File tree = new File(outputFile);

            BufferedWriter bufferWriter = new BufferedWriter(new java.io.FileWriter(tree));
            for (int i = 1; i <= U; i++) {
                //    freqs.put(i,avgs[numberOfDomain+i]);
                bufferWriter.write( i + "," + Math.round(avgs[U + i]) + "\n");
            }
            //    System.out.println("frequencies <indexOfElement,frequency>: "+freqs);
            //      java.util.Iterator it = freqs.entrySet().iterator();
            //          File tree=new File(fileName.substring(fileName.lastIndexOf("\\")+1,fileName.lastIndexOf("."))+"FreqTree.txt");


//            while(it.hasNext()) {
//                HashMap.Entry entry = (HashMap.Entry) it.next();
//                Integer key = (Integer) entry.getKey();
//                Double value = (Double) entry.getValue();  // 返回与此项对应的值
//                bufferWriter.write("("+key+","+value+")\n");
//            }
            bufferWriter.flush();
            bufferWriter.close();
        } catch (
                Exception e)

        {
            e.printStackTrace();
        }
    }

}
