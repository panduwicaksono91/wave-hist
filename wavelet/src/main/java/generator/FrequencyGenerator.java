package main.java.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class FrequencyGenerator {
    public static void main(String[] args) {
        DataSet a = generateFrequency("E://96-test//1.txt");
        try {
            a.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static DataSet<Tuple2<String, Integer>> generateFrequency(String filepath) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(filepath);
        DataSet<Tuple2<String, Integer>> frequency = text.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        // normalize and split the line
                        String[] tokens = value.split("\\W+|,");

                        // emit the pairs
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }
                    }
                }
        ).groupBy(0)
                .sum(1).sortPartition(0, Order.ASCENDING).setParallelism(1);;

        return frequency;
    }
}
