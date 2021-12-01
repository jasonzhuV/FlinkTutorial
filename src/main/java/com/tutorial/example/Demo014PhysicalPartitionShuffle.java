package com.tutorial.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 将数据分到不同的任务槽
 * 逻辑分区 逻辑上将数据分到不同的分区，但是不同分区的数据可能是在一个任务槽的，每个逻辑分区维护自己的累加器，逻辑分区有时候比物理分区更有意义
 * 随机分区
 */
public class Demo014PhysicalPartitionShuffle {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
                .setParallelism(1)
                .shuffle()
                .print();// TODO 不指定并行度，默认是机器的核数



        env.execute();
    }
}
