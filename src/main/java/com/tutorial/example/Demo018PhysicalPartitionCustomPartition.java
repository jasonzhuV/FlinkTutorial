package com.tutorial.example;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区
 * 自定义分区
 */
public class Demo018PhysicalPartitionCustomPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(
                        // 分区器 根据key计算分区号
                        new Partitioner<Integer>() {
                            // Returns: The partition index.
                            // 返回值是int类型的任务槽索引，从0开始 index=0 是1号分区
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key % 2;
                            }
                        },
                        // key选择器 <IN, KEY>
                        new KeySelector<Integer, Integer>() {
                            // 输入：The object to get the key from
                            // 输出：提取的key
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                return value;
                            }
                        }
                )
                .print();

        env.execute();
    }
}
