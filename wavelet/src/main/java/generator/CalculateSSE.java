package main.java.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class CalculateSSE {
    public static void main(String[] args) throws Exception {
        //file1,file2 are two files for calcualting SSE, file3 is outputpath.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String file1=args[0];
        String file2=args[1];
        String file3=args[2];
//        String file1="wave-hist\\wavelet\\src\\resource\\freqs\\toydataset_1_freq.txt";
//        String file2="wave-hist\\wavelet\\src\\resource\\freqs\\basicscoeffsfreqs.txt";
        DataSet<String> text = env.readTextFile(file1);
        DataSet<String> text2 = env.readTextFile(file2);

        DataSet<Tuple2<String, Long>> t1 = text.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        String[] tokens = value.replace("(", "").replace(")", "").split("\\W+|,");
                        out.collect(new Tuple2<String, Long>(tokens[0], Long.valueOf(tokens[1])));
                    }
                }
        );
        DataSet<Tuple2<String, Long>> t2 = text2.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        String[] tokens = value.replace("(", "").replace(")", "").split("\\W+|,");
                        out.collect(new Tuple2<String, Long>(tokens[0], Long.valueOf(tokens[1])));
                    }
                }
        );
        DataSet<Tuple2<Tuple2<String, Long>, Tuple2<String, Long>>> result = t1.join(t2).where(0).equalTo(0);
        DataSet<Tuple1<Long>> sse = result.flatMap(
                new FlatMapFunction<Tuple2<Tuple2<String, Long>, Tuple2<String, Long>>, Tuple1<Long>>() {
                    @Override
                    public void flatMap(Tuple2<Tuple2<String, Long>, Tuple2<String, Long>> value, Collector<Tuple1<Long>> out) {
                        Long res = Math.round(Math.pow(value.f0.f1 - value.f1.f1, 2));


                        out.collect(new Tuple1<Long>(res));
                    }
                }
        ).sum(0);
//        sse.print();
        System.out.println("start writing");
        sse.writeAsText(file3, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();


    }
}
