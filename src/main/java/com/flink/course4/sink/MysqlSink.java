package com.flink.course4.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MysqlSink {
    public static void main(String[] args) throws Exception {
        //local模式默认并行度是当前机器的逻辑核数的数量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //
        int parallelism = env.getParallelism();
        System.out.println("系统默认的并行度"+parallelism);
        //
        DataStreamSource<String> lines = env.socketTextStream("localhost", 999);

        int parallelism1 =lines.getParallelism();
        System.out.println("source的并行度"+parallelism1);

        //调用transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        //聚合
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp2) throws Exception {
                return tp2.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        
        env.execute("mysql sink job");
    }
}
