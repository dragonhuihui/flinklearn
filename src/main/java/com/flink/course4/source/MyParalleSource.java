package com.flink.course4.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

//并行的source,实现parallelsourcefunction
public class MyParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String > numberStream = env.addSource(new MySource4());
        SingleOutputStreamOperator<String> dataStream = numberStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("接受到了数据："+value);
                return value;
            }
        });
       dataStream.print().setParallelism(1);
        env.execute("并行");
    }
    public static  class  MySource4 extends RichParallelSourceFunction<String> {
        //1.调用mysource4的构造方法
        //2.调用open方法，调用一次
        //3.调用run方法
        //4.调用cancel方法停止
        //5.调用close方法
        public MySource4(){
            System.out.println("construct invoked");
        }
       private Boolean flag=true;
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open method invoked");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close method invoed");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
                while (flag){
                    ctx.collect(UUID.randomUUID().toString());
                    Thread.sleep(20000);
                }
        }

        @Override
        public void cancel() {
         flag=false;
        }


    }

}
