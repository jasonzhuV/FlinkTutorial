package com.tutorial.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区
 * 随机分区
 */
public class Demo_014_PhysicalPartition_Shuffle {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
                .setParallelism(1)
                .shuffle()
                .print();// 这里不指定并行度，默认是机器的核数



        env.execute();
    }
}
