package com.flink.course4.splitstream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * join 就是cogroup特殊
 */
public class join {
    public static void main(String[] args) throws Exception {



        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });
        DataStream<String> joinedStream = s1.join(s2)
                .where(tp2 -> tp2.f0)
                .equalTo(tp3 -> tp3.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1, Tuple3<String, String, String> t2) throws Exception {
                        return t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2;
                    }
                });

        joinedStream.print();



        env.execute("join  job");
    }
}
