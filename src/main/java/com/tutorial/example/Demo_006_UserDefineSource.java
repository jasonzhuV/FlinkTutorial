package com.tutorial.example;

import com.tutorial.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源，用户行为数据
 */
public class Demo_006_UserDefineSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource()).print();
        env.execute("UserDefineSource");
    }
}
