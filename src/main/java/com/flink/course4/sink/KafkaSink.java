package com.flink.course4.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class KafkaSink {
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

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("dev0:2181", "wordcount18", new SimpleStringSchema());

        //写入到kafka 中
        lines.addSink(kafkaProducer);
        env.execute("kafka sink job");
    }
}
