package com.flink.course4.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义单并行度的source,自定义单并行度的source需要实现sourceFunction接口，并行度为1。
 */
public class MyNoParalleSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> numberStream = env.addSource(new Mysouce1()).setParallelism(1);
        SingleOutputStreamOperator<Long> dataStream = numberStream.map(new MapFunction<Long, Long>() {
             @Override
             public Long map(Long value) throws Exception {
              System.out.println("接受到了数据："+value);
               return value;
           }
          });
        SingleOutputStreamOperator<Long> filterDataStream =
                dataStream.filter(new FilterFunction<Long>() {
                    @Override
                    public boolean filter(Long number) throws Exception {
                        return number % 2 == 0;
                    }
                });
        filterDataStream.print().setParallelism(1);
        env.execute("StreamingDemoWithMyNoPralalleSource");
    }

    public static class Mysouce1 implements  SourceFunction<Long>{
        //每次来一条数据都会调用一次running方法
        private long number = 1L;
        private boolean isRunning = true;
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while(isRunning){
                sourceContext.collect(number);//输出
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
}

