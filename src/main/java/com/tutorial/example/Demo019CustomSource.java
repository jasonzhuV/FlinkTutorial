package com.tutorial.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义数据源
 * fromElements 算子的并行度必须是 1
 * ① 要保证顺序不乱
 * ② socket也是
 * ③ 之前写的Source并行度也只能是1
 * env.fromElements(1,2,3).setParallelism(2).print();
 */
public class Demo019CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new IntegerSource()).setParallelism(2).print();
//        env.addSource(new RichIntegerSource()).setParallelism(2).print();

        env.execute();
    }

    /**
     * 支持多并行度的数据源
     */
    public static class IntegerSource implements ParallelSourceFunction<Integer> {

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            sourceContext.collect(1);
            sourceContext.collect(2);
            sourceContext.collect(3);
            sourceContext.collect(4);
        }

        @Override
        public void cancel() {

        }
    }

    public static class RichIntegerSource extends RichParallelSourceFunction<Integer> {

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            sourceContext.collect(1);
            sourceContext.collect(2);
            sourceContext.collect(3);
            sourceContext.collect(4);
        }

        @Override
        public void cancel() {

        }
    }
}
