package com.tutorial.dolphinscheduler;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : zhupeiwen 自定义数据源，用户行为数据
 */
public class FlinkInDolphin {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new DolphinSource()).print();

        env.execute("FlinkInDolphin");
    }
}
