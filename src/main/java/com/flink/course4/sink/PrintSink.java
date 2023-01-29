package com.flink.course4.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import scala.reflect.reify.phases.Calculate;

/**
 * @description:
 * @author: 龙辉辉
 * @create: 2021-11-07 15:42
 */
public class PrintSink {
    public static void main(String[] args) throws Exception {
        //local模式默认并行度是当前机器的逻辑核数的数量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //
        int parallelism = env.getParallelism();
        System.out.println("系统默认的并行度"+parallelism);
        //
        DataStreamSource<String> lines = env.socketTextStream("localhost", 999);

        int parallelism1 =lines.getParallelism();
        System.out.println("source的并行度"+parallelism1);

        //调用完成后并行度
        int parallelism2 = lines.getParallelism();
        System.out.println("调用完成后的并行度"+parallelism2);
        //
        lines.addSink(new MyPrintSink()).name("my-print-sink");
        //
        env.execute("socket job");
    }
    public static class  MyPrintSink extends RichSinkFunction<String>{

     private  int indexOfThisSubtask;
        @Override
        public void open(Configuration parameters) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
             indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

             //最后数据输出的逻辑。你要把数据输出到Hdfs上，这里就是Hdfs逻辑，输出mysql上那就是mysql的逻辑。
            System.out.println(value);


            System.out.println(indexOfThisSubtask+"> "+value);
        }
    }
}
