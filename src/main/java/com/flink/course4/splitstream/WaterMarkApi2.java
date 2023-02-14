package com.flink.course4.splitstream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class WaterMarkApi2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 1,e01,168673487846,pg01
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
                    String[] split = s.split(",");
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                }).returns(EventBean.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EventBean>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                                    @Override
                                    public long extractTimestamp(EventBean eventBean, long recordTimestamp) {
                                        return eventBean.getTimeStamp();
                                    }
                                })
                ).setParallelism(2);

        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

                Thread.sleep(1000);
                System.out.println("睡醒了，准备打印");

                // 打印此刻的 watermark
                long processTime = ctx.timerService().currentProcessingTime();
                long watermark = ctx.timerService().currentWatermark();

                System.out.println("本次收到的数据" + eventBean);
                System.out.println("此刻的watermark： " + watermark);
                System.out.println("此刻的处理时间（processing time）： " + processTime );

                out.collect(eventBean);
            }
        }).setParallelism(1).print();


        env.execute("watermark api 2");
    }
}
