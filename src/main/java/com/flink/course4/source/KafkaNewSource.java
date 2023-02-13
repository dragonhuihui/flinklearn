package com.flink.course4.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedValueDeserializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaNewSource {
    public static void main(String[] args) throws Exception {
        //local模式默认并行度是当前机器的逻辑核数的数量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // 设置订阅的目标主题
                .setTopics("tp01")

                // 设置消费者组id
                .setGroupId("gp01")

                // 设置kafka服务器地址
                .setBootstrapServers("doit01:9092")

                // 起始消费位移的指定：
                //    OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 消费起始位移选择之前所提交的偏移量（如果没有，则重置为LATEST）
                //    OffsetsInitializer.earliest()  消费起始位移直接选择为 “最早”
                //    OffsetsInitializer.latest()  消费起始位移直接选择为 “最新”
                //    OffsetsInitializer.offsets(Map<TopicPartition,Long>)  消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 设置value数据的反序列化器
                //如果只有value的话,设置的成onlyvalue
                // ValueDeserializerWrapper
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                // 开启kafka底层消费者的自动位移提交机制
                //    它会把最新的消费位移提交到kafka的consumer_offsets中
                //    就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                //    （宕机重启时，优先从flink自己的状态中去获取偏移量<更可靠>）
                .setProperty("auto.offset.commit", "true")

                // 把本source算子设置成  BOUNDED属性（有界流）
                //     将来本source去读取数据的时候，读到指定的位置，就停止读取并退出
                //     常用于补数或者重跑某一段历史数据
                // .setBounded(OffsetsInitializer.committedOffsets())

                // 把本source算子设置成  UNBOUNDED属性（无界流）
                //     但是并不会一直读数据，而是达到指定位置就停止读取，但程序不退出
                //     主要应用场景：需要从kafka中读取某一段固定长度的数据，然后拿着这段数据去跟另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest())

                .build();

        // env.addSource();  //  接收的是  SourceFunction接口的 实现类
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");//  接收的是 Source 接口的实现类
        streamSource.print();


        env.execute();
    }
}
