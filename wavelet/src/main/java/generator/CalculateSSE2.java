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

public class CalculateSSE2 {
    //file1 is file directory path, file3 is outputpath
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String file1=args[0];
        String file3=args[1];

//        String file1="wave-hist\\wavelet\\src\\resource\\toydataset_1_freq.txt";
//        String file2="wave-hist\\wavelet\\src\\resource\\test.txt";
        DataSet<String> text = env.readTextFile(file1);
//        DataSet<String> text2 = env.readTextFile(file2);

        DataSet<Tuple2<Integer, Integer>> t1 = text.flatMap(
                new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                        String[] tokens = value.replace("(", "").replace(")", "").split("\\W+|,");
                        out.collect(new Tuple2<Integer, Integer>(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1])));
                    }
                }
        );
//        DataSet<Tuple2<String, Integer>> t2 = text2.flatMap(
//                new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//                        String[] tokens = value.replace("(", "").replace(")", "").split("\\W+|,");
//                        out.collect(new Tuple2<String, Integer>(tokens[0], Integer.valueOf(tokens[1])));
//                    }
//                }
//        );
     //   DataSet<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> result = t1.join(t2).where(0).equalTo(0);
        DataSet<Tuple1<Long>> sse = t1.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple1<Long>>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple1<Long>> collector) throws Exception {
                int i=1;
                int value=0;

                for (Tuple2<Integer, Integer> t : iterable) {
          //          System.out.println("t:0/1 "+ t.f0+" "+t.f1);
                    value+=t.f1*i;
                    i=i*(-1);
            //        System.out.println("value: "+value);

                }

                //   divided by pp, to get unbiased estimator
                long partialsse=(int)Math.pow(value,2);
                collector.collect(Tuple1.of(partialsse));
            }
        }).sum(0);
      //  sse.print();
        sse.writeAsText(file3, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();


    }
}
