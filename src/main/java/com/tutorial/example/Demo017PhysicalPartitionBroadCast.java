package com.tutorial.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author     : zhupeiwen
 * 物理分区
 * 广播： 复制传播，广播到所有节点
 * 不叫广播变量，叫广播流，这条流会广播出去，流中一个一个的元素会根据下游的算子数复制后传递
 */
public class Demo017PhysicalPartitionBroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .broadcast()
                .print().setParallelism(2);

        env.execute();
    }
}
