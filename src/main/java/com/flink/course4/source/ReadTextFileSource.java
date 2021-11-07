package com.flink.course4.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//readtextfile 创建的是多并行的source数据流。是一个有界的数据流
public class ReadTextFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        String path="file:///Users/longhuihui/Desktop/flinklearn/data/input.txt";
        //readFile(format, filePath, FileProcessingMode.PROCESS_ONCE, -1, typeInfo);
        //底层调用的是readfile当数据读变化的数据，读变化数据，并且时间-1，就是只读一次。
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);
        int parallelism = dataStreamSource.getParallelism();
        System.out.println("读入数据的数据"+parallelism);
        dataStreamSource.print();
        env.execute("读取文件系统的数据");

    }
}
