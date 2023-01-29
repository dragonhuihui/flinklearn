package com.flink.course4.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KakaSource {
    public static void main(String[] args) throws Exception {
        //创建本地运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //创建消费者消费kafka的消息
        // 设置并行度1
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dev0:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //偏移量--earliest 如果没有记录偏移量，就从头读，如果记录过偏移量，就接着读
        properties.setProperty("auto.offset.reset", "latest");
        //没有开checkpoint，让flink提交偏移量的消费者定期提交偏移量
        properties.setProperty("enable.auto.commit","true");
        // flink添加外部数据源  topic 序列化
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(),properties));
        // 打印输出
        dataStream.print();

        env.execute("kafkasource");
    }
}
