package com.flink.course4.splitstream;

import com.flink.course4.EventLog;
import com.flink.course4.source.MyNoParalleSource;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import com.alibaba.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SideOutput {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);


        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 需求： 将行为事件流，进行分流
        //     appLaunch 事件 ，分到一个流
        //     putBack 事件，分到一个流
        //     其他事件保留在主流
        SingleOutputStreamOperator<EventLog> processed = streamSource.process(new ProcessFunction<EventLog, EventLog>() {
            /**
             *
             * @param eventLog  输入数据
             * @param ctx 上下文，它能提供“测输出“功能
             * @param out 主流输出收集器
             * @throws Exception
             */
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, EventLog>.Context ctx, Collector<EventLog> out) throws Exception {
                String eventId = eventLog.getEventId();
                  //放到前面是防止空指针，
                // 需求： 将行为事件流，进行分流
                //     appLaunch 事件 ，分到一个流
                //    所有数据输出到主流
                if ("appLaunch".equals(eventId)) {

                    ctx.output(new OutputTag<EventLog>("odd", TypeInformation.of(EventLog.class)), eventLog);

                } else if ("putBack".equals(eventId)) {

                   ctx.output(new OutputTag<String>("even",TypeInformation.of(String.class)), JSON.toJSONString(eventLog));
                }

                out.collect(eventLog);

            }
        });

        // 获取  launch 测流数据
        DataStream<EventLog> launchStream = processed.getSideOutput(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)));

        // 获取back 测流数据
        DataStream<String> backStream = processed.getSideOutput(new OutputTag<String>("back",TypeInformation.of(String.class)));

        launchStream.print("launch");

        backStream.print("back");


        env.execute("sideoutput");


    }
     static class MySourceFunction implements SourceFunction<EventLog> {
        volatile boolean flag = true;

        @Override
        public void run(SourceContext<EventLog> ctx) throws Exception {

            EventLog eventLog = new EventLog();
            String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
            HashMap<String, String> eventInfoMap = new HashMap<>();

            while(flag){

                eventLog.setGuid(RandomUtils.nextLong(1,1000));
                eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
                eventLog.setTimeStamp(System.currentTimeMillis());
                eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);

                eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
                eventLog.setEventInfo(eventInfoMap);

                ctx.collect(eventLog);

                eventInfoMap.clear();

                Thread.sleep(RandomUtils.nextInt(200,1500));
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }


}
