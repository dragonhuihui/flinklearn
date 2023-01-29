package com.flink.course4.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * 多并行度的source,有限的数据
 * 单并行度的source直接实现的是sourcefunction 接口
 * 多并行度的source可以通过实现richparallelSourceFunction或者parallelSourceFunction
 */
public class FromParCollection {
    public static void main(String[] args) throws Exception {

       Configuration configuration= new Configuration();
       //设置web ui 的端口号
       configuration.setInteger("rest.port",8081); //设置web ui的端口号

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
         //LongValueSequenceIterator是一个迭代器，
        // 它返回一个数字序列(如LongValue)。
        // 迭代器是splittable(由SplittableIterator定义，也就是说，它可以分为多个迭代器，每个迭代器返回数字序列的子序列。
        //SplittableIterator<OUT> iterator-->实现类是number,long
        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1l, 20l), long.class);
        //底层调用的是addsource
        System.out.println("多并行度的数据"+nums.getParallelism());
        nums.print();
        env.execute("多并行度的数据");
    }
}
