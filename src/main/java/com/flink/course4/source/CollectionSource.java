package com.flink.course4.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 集合中创建source--单并行度，从集合中创建
 */
public class CollectionSource {

    public static void main(String[] args) throws Exception {


        //local模式默认并行度是当前机器的逻辑核数的数量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //从集合中创建数据流
        DataStream<Integer> DataStream = env.fromCollection(
                Arrays.asList(1,2,4,5,6,7,8,9,10));
        System.out.println("-并行度为-"+DataStream.getParallelism());
        DataStream.print();
        env.execute("集合创建数据流");
    }
}
