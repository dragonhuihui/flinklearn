package com.flink.course4.splitstream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * cogroup
 */
public class CoGroup {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        DataStreamSource<String>  stream1 = env.fromElements("1", "2,bb,m,28", "3,cc,f,38");
        DataStreamSource<String>  stream2 = env.fromElements("2", "2:bb:m:28", "3:cc:f:38");

        stream1.coGroup(stream2).where(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
               // System.out.println(value);
                return value;
            }
        }).equalTo(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                System.out.println(value);
                return value;
            }
            //必须开窗口
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(30))).apply(
                new CoGroupFunction<String, String, String>() {
                    @Override
                    public void coGroup(Iterable<String> first, Iterable<String> second, Collector<String> out) throws Exception {
                        //拼接两个字段。
                        for(String t1:first){
                            System.out.println(t1);
                            boolean flag=false;
                            //left join
                            //遍历在
                            for(String  t2:second){
                                out.collect(t1+":"+t2);
                                flag=true;
                              //  System.out.println(t1);
                            }
                            //右表没有数据
                            if(!flag){
                                out.collect(t1+":"+null);
                              //  System.out.println(t1);
                            }
                        }
                    }
                }
        ).print();
        env.execute("cogroup job");
    }
}
