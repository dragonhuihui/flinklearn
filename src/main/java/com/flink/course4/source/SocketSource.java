package com.flink.course4.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description: socketsource
 * @author: 龙辉辉
 * @create: 2021-11-06 17:51
 */
public class SocketSource {
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
        //1.1行变成多行
       SingleOutputStreamOperator<String> words= lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        //调用完成后并行度
         int parallelism2 = words.getParallelism();
        System.out.println("调用完成后的并行度"+parallelism2);
        //
        words.print();
        //
        env.execute("socket job");
    }
}
