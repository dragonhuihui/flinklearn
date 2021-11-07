package com.flink.course4.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
//TextInputFormat多并行度的文件数据--对并行source,无限的数据流，
public class ReadFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        String path="file:///Users/longhuihui/Desktop/flinklearn/data/input.txt";
        //FileProcessingMode.PROCESS_ONCE）当文件变化的时候，把发生变化的数据读入到flink系统中。
        //一旦文件发生变化，flink把变化的全部加入到flink系统中 FileProcessingMode.PROCESS_CONTINUOUSLY--全部读 --不是增量
        //readFile(fileInputFormat, path) -根据指定的文件输入格式读取（一次）文件。
        //readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo)
        DataStreamSource<String> dataStreamSource = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);
        dataStreamSource.print();
        int parallelism = dataStreamSource.getParallelism();
        System.out.println("读入数据的数据"+parallelism);
        env.execute("读取文件系统的数据");
    }
}
