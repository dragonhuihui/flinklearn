package com.flink.course4.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 *  增量聚合 aggreate
 *  1,e01,10000,p01,10
 *  1,e02,11000,p02,20
 *  1,e02,12000,p03,40
 *  1,e03,20000,p02,10
 *  1,e01,21000,p03,50,
 *  1,e04,22000,p04,10
 *  1,e06,28000,p05,60
 *  1,e07,30000,p02,10
 */
public class WindowApi2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean2> beanStream = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        });
        // 分配 watermark ，以推进事件时间
        SingleOutputStreamOperator<EventBean2> watermarkedBeanStream  = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
            @Override
            public long extractTimestamp(EventBean2 eventBean2, long l) {
                return eventBean2.getTimeStamp();
            }
        }));
        /**
         * 滚动聚合api使用示例
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
         * 使用aggregate算子来实现
         */
        watermarkedBeanStream.keyBy(EventBean2::getGuid).window(
                //参数1:窗口长度，参数2 滑动步长
                SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10))

        ).aggregate(
                //参数1:输入数据类型，参数2:参数累加类型，参数3:数据输出类型
                new AggregateFunction<EventBean2, Integer, Integer>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }
                    /**
                     * 滚动聚合的逻辑（拿到一条数据，如何去更新累加器）
                     * @param value The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public Integer add(EventBean2 value, Integer accumulator) {
                        return  accumulator+1;
                    }

                    /**
                     * 从累加器中，计算出最终要输出的窗口结算结果
                     * @param accumulator The accumulator of the aggregation
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }
                    /**
                     *  批计算模式下，可能需要将多个上游的局部聚合累加器，放在下游进行全局聚合
                     *  因为需要对两个累加器进行合并
                     *  这里就是合并的逻辑
                     *  流计算模式下，不用实现！
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a+b;
                    }
                }
        ).print();
                /*
                reduce 滚动聚合算子，它有个限制，聚合结果的数据类型与数据源的数据类型一致。
                .reduce(new ReduceFunction<EventBean2>() {
            @Override
            public EventBean2 reduce(EventBean2 eventBean2, EventBean2 t1) throws Exception {
                return  null;
            }
        })

                 */
        env.execute("window api");

    }
}
