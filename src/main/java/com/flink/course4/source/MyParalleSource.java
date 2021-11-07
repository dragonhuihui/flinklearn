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
//        SingleOutputStreamOperator<> filterDataStream =
//                dataStream.filter(new FilterFunction<Long>() {
//                    @Override
//                    public boolean filter(Long number) throws Exception {
//                        return number % 2 == 0;
//                    }
//                });
       dataStream.print().setParallelism(1);
        env.execute("并行");
    }
    //这样的数据会产生4份，每个并行度都会产生一份。相当于4份，同一份数据--多个实例，
    public static class MySource2 implements ParallelSourceFunction{

        //每次来一条数据都会调用一次running方法
        private long number = 1L;
        private boolean isRunning = true;
        @Override
        public void run(SourceContext ctx) throws Exception {
            while(isRunning){
                ctx.collect(number);//输出
                number++;
            }
            //每秒中生成一条数据
            Thread.sleep(1000);
        }

        @Override
        public void cancel() {
            isRunning=false;
        }
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
