package com.flink.course4.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * map 函数源码解析
 */
public class MapSourceCode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);

        //操作的名字，输出数据类型,

//        SingleOutputStreamOperator<Integer> my_mapfunction = source.transform("My mapfunction", TypeInformation.of(Integer.class), new StreamMap<>(new MapFunction<Integer, Integer>() {
//
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value * 2;
//            }
//        }));

        SingleOutputStreamOperator<Integer> result = source.transform("My mapfunction", TypeInformation.of(Integer.class), new MyStreamMap());

        result.print();
        env.execute("source code");

    }
    public static class MyStreamMap extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer,Integer>{

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {

            Integer value = element.getValue();
            Integer j=value*2;
            element.replace(j);
            output.collect(element);
        }
    }
}
