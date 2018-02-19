package main.java.generator;

import java.io.BufferedWriter;
import java.io.File;



import java.io.BufferedReader;

import java.io.FileReader;
import java.util.HashMap;

//rebuild the frequency with coefficients and number of domain.
//generate a map of frequencies,<indexOfElement,frequency>
public class ReproduceFrequency {
    public static void main(String[] args) throws Exception {
        //example file:(8,3) firstline, (numberOfDomain, numberOfCoefficient)
       // (3,2.5)  (indexOfCoefficentTree, coefficientValue)
        //(1,6.75)
        //(4,5.0)
        //2^29 =
        String inputFile = args[0];
        reproduceFrequency(inputFile,8);
        System.out.println(inputFile.substring(inputFile.lastIndexOf("\\")+1,inputFile.lastIndexOf(".")));

//        reproduceFrequency("wave-hist\\wavelet\\src\\resource\\basicscoeffs.txt",8);
    //    reproduceFrequency("wave-hist\\wavelet\\src\\resource\\basicscoeffs.txt",(int)Math.pow(2,29));

    }

//    public static double SSE(HashMap<Integer,Integer> trueValue, HashMap<Integer,Double> predictedValue){
//        int n = trueValue.size();
//        double a = 0.0;
//        for (int i = 0; i < n; i++) {
//            a+=Math.pow(trueValue.get(i)-predictedValue.get(i),2);
//        }
//        return Math.sqrt(a/n);
//    }

    public static HashMap<Integer,Double> reproduceFrequency(String fileName,int numberOfDomain) {
        File file = new File(fileName);
        BufferedReader reader = null;
 //       int numberOfDomain = 0;
        HashMap<Integer, Double> wavelet = new HashMap<Integer, Double>();
        HashMap<Integer,Double> avgs = new HashMap<Integer , Double>();
        HashMap<Integer,Double> freqs = new HashMap<Integer , Double>();
        int level = 0;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                System.out.println("line " + line + ": " + tempString);
                String[] substring = tempString.split("[\\(\\)\\s+,]");
//                if (line == 1) {
//                    numberOfDomain = Integer.valueOf(substring[1]);
//                    numberOfK = Integer.valueOf(substring[2]);
//                } else {
                    wavelet.put(Integer.valueOf(substring[1]), Double.valueOf(substring[2]));
//                }
                line++;
            }
            reader.close();
            level = (int) (Math.log(numberOfDomain) / Math.log(2));

            if (wavelet.containsKey(1)) {
                avgs.put(1, wavelet.get(1));
                avgs.put(2, wavelet.get(1));
            } else {
                avgs.put(1, 0.0);
                avgs.put(2, 0.0);
            }

            double coJ=0;
            for (int i = 1; i <= level; i++) {

                for (int j = (int)Math.pow(2,i-1)+1; j <= (int)Math.pow(2,i); j++) {

                    if(wavelet.containsKey(j)){
                        coJ=wavelet.get(j);
                    }else{
                        coJ=0;
                    }

                    avgs.put(j*2-1,avgs.get(j)-coJ);
                    avgs.put(j*2,avgs.get(j)+coJ);
                    avgs.remove(j);
                }
            }
     //       System.out.println(avgs);
            for(int i=1;i<=numberOfDomain;i++){
                freqs.put(i,avgs.get(numberOfDomain+i));
            }
            System.out.println("frequencies <indexOfElement,frequency>: "+freqs);
            java.util.Iterator it = freqs.entrySet().iterator();
            File tree=new File(fileName.substring(fileName.lastIndexOf("\\")+1,fileName.lastIndexOf("."))+"FreqTree.txt");
            BufferedWriter bufferWriter = new java.io.BufferedWriter(new java.io.FileWriter(tree));
            while(it.hasNext()) {
                java.util.HashMap.Entry entry = (java.util.HashMap.Entry) it.next();
                Integer key = (Integer) entry.getKey();
                Double value = (Double) entry.getValue();  // 返回与此项对应的值
                bufferWriter.write("("+key+","+value+")\n");
            }
            bufferWriter.flush();
            bufferWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return freqs;
    }

}
