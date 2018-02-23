package main.java.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

//a method that generate frequencies of words, only for future test.
// the BaselineImpl already implemented the caculation.
public class FrequencyGenerator {
    public static void main(String[] args) {
        String in = String.valueOf(args[0]);
        String out = String.valueOf(args[1]);
        System.out.println("frequency generate");

        System.out.println(in);
        System.out.println(out);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(in);
        DataSet<Tuple2<Integer, Integer>> frequency = text.flatMap(
                new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                        // normalize and split the line
                        String[] tokens = value.split("\\W+|,");

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