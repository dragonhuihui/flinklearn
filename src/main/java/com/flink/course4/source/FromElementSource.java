package com.flink.course4.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从元素中创建集合--单并行度
 */
public class FromElementSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //创建数据流--从可变参数
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 8);
        System.out.println("--单并行度的source"+dataStreamSource.getParallelism());
        dataStreamSource.print();
        env.execute("可变集合中创建对象");
    }
}
