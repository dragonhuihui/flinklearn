package com.flink.course4.splitstream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMarkApi {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        // 1,e01,168673487846,pg01
        SingleOutputStreamOperator<String> s1 = env.socketTextStream("localhost", 9999).disableChaining();
       // s1.assignTimestampsAndWatermarks()

        // 策略1： WatermarkStrategy.noWatermarks()  不生成 watermark，禁用了事件时间的推进机制
        // 策略2： WatermarkStrategy.forMonotonousTimestamps()  紧跟最大事件时间
        // 策略3： WatermarkStrategy.forBoundedOutOfOrderness()  允许乱序的 watermark生成策略
        // 策略4： WatermarkStrategy.forGenerator()  自定义watermark生成算法

        /*
         * 示例 一 ：  从最源头算子开始，生成watermark
         */
        // 1、构造一个watermark的生成策略对象（算法策略，及事件时间的抽取方法）
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                //string 数据类型的定义
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))  // 允许乱序的算法策略
                //带时间戳的从哪里来。--数据从哪里抽取时间戳
                .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[2]));  // 时间戳抽取方法
        // 2、将构造好的 watermark策略对象，分配给流（source算子）
        /*s1.assignTimestampsAndWatermarks(watermarkStrategy);  返回的是一个流*/





        /*
         * 示例 二：  不从最源头算子开始生成watermark，而是从中间环节的某个算子开始生成watermark
         * 注意！：如果在源头就已经生成了watermark， 就不要在下游再次产生watermark
         */
        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
                    String[] split = s.split(",");
                    /* Thread.sleep(50000);*/
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                }).returns(EventBean.class).disableChaining()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EventBean>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                                    @Override
                                    public long extractTimestamp(EventBean eventBean, long recordTimestamp) {
                                        return eventBean.getTimeStamp();
                                    }
                                })
                );

        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

                /*Thread.sleep(1000);
                System.out.println("睡醒了，准备打印");*/

                // 打印此刻的 watermark
                long processTime = ctx.timerService().currentProcessingTime();
                long watermark = ctx.timerService().currentWatermark();
                System.out.println("本次收到的数据" + eventBean);
                System.out.println("此刻的watermark： " + watermark);
                System.out.println("此刻的处理时间（processing time）： " + processTime );

                out.collect(eventBean);
            }
        }).startNewChain().print();


        env.execute("Watermark API");


    }
}
@Data
@NoArgsConstructor
@AllArgsConstructor
class EventBean {
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
}
