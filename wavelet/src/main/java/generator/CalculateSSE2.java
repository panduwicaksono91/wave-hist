package main.java.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author Shibo Cheng
 * calculate SSE without join method,only by reduce, perform worse than CalculateSSE in experiment
 * specify input is a directory, and only contains two files that need to be calculated
 */
public class CalculateSSE2 {
    /**
     * calculate SSE, write to outputfile path
     *
     * @param args inputdirectory path,outputfile path
     */
    public static void main(String[] args) throws Exception {
        String fileFolder = args[0];
        String outputFile = args[1];
        sse2(fileFolder, outputFile);

    }

    /**
     * calculate SSE
     * @param fileFolder inputdirectory path
     * @param outputFile sse result
     * @throws Exception
     */
    public static void sse2(String fileFolder, String outputFile) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(fileFolder);
        DataSet<Tuple2<Integer, Integer>> t1 = text.flatMap(
                new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                        String[] tokens = value.replace("(", "").replace(")", "").split("\\W+|,");
                        out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1])));
                    }
                }
        );
        DataSet<Tuple1<Long>> sse = t1.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple1<Long>>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple1<Long>> collector) throws Exception {
                int i = 1;
                int value = 0;
                int count = 0;
                for (Tuple2<Integer, Integer> t : iterable) {
                    value += t.f1 * i;
                    i = i * (-1);
                    count++;
                }
                long partialsse = (int) Math.pow(value, 2);
                //if a key only exists in one file, then ignore it.
                if (count == 2) {
                    collector.collect(Tuple1.of(partialsse));
                }
            }
        }).sum(0);
        sse.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();

    }
}
