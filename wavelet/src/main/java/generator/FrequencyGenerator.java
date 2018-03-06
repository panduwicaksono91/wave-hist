package main.java.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


/**
 * @author Shibo Cheng
 * generate frequencies of each items
 */
public class FrequencyGenerator {
    /**
     * generate frequencies of each items in inputfile, write to outputfile path
     *
     * @param args inputfile path, outputfile path
     */
    public static void main(String[] args) {
        String in = String.valueOf(args[0]);
        String out = String.valueOf(args[1]);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(in);
        DataSet<Tuple2<Integer, Integer>> frequency = text.flatMap(
                new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                        // normalize and split the line
                        String[] tokens = value.replace("(", "").replace(")", "").split("\\W+|,");
                        // emit the pairs
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(token), 1));
                            }
                        }
                    }
                }
        ).groupBy(0)
                .sum(1);
        try {
            frequency.writeAsText(out, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}