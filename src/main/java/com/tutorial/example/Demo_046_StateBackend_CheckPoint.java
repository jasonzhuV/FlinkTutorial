package com.tutorial.example;

import com.tutorial.source.ClickSource;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author     : zhupeiwen
 * date       : 2021/11/17 3:41 下午
 * description:
 */
public class Demo_046_StateBackend_CheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10 * 1000L);
        /*
         * 1.13 之后 CheckPoint 的持久化方式写法有变化
         * state backend
         *      HashMapStateBackend
         *      EmbeddedRocksDBStateBackend
         * checkpoint storage types
         *      JobManagerCheckpointStorage
         *      FileSystemCheckpointStorage
         * 写法如下，需要指定state backend 和 checkpoint storage types
         * If a checkpoint directory is configured FileSystemCheckpointStorage will be used, otherwise the system will use the JobManagerCheckpointStorage.
         * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/state/checkpoints/
         */
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/jason/MyProjects/FlinkTutorial/src/main/resources/checkpoints");



        /*
         * 1.12 及之前版本写法
         */
        env.setStateBackend(new FsStateBackend("file:///Users/jason/MyProjects/FlinkTutorial/src/main/resources/checkpoints", false));


        env
                .addSource(new ClickSource())
                .print();

        env.execute();

    }
}
