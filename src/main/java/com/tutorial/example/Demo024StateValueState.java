package com.tutorial.example;

import com.tutorial.source.LongSource;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * ValueState 值状态变量的例子
 * 数据从一开始上升开始，连续1s上升，则报警
 */
public class Demo024StateValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new LongSource())
                .keyBy(r -> true)
                .process(new LongIncreaseAlert())
                .print();

        env.execute();
    }

    /*
     * Long 型数字连续1s上升则报警
     */
    public static class LongIncreaseAlert extends KeyedProcessFunction<Boolean, Long, String> {
        // 声明值状态变量
        private ValueState<Long> lastNumber; // 最近一次的数字
        private ValueState<Long> timerTs; // 用来存储定时器的时间戳

        /*
         * 针对每一个并行任务，或者说在每一个任务槽上执行一次 open 方法
         * 底层是用map来维护状态变量，因为键控流的不同key对应的数据可能在一个任务槽上
         * 而且对于每一个key分开维护状态变量，其实是利用mao做逻辑隔离，对于不同的key分开维护状态变量
         * 可以理解为 底层的map名称为状态变量的名字（"last-number"） 然后对于每一个key的状态变量用键控流各自的key区分
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化状态变量
            // 1. 状态变量是单例模式，意味着状态变量只会被初始化一次
            // 2. 状态变量会定期存储到检查点（例如HDFS）
            // 3. 当程序重启时会使用名字去检查点里找这个变量，如果找到直接读取，找不到再初始化，重启多少次都只有一个单例
            lastNumber = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("last-number", Types.LONG)
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );

        }

        @Override
        public void processElement(Long in, Context context, Collector<String> collector) throws Exception {
            Long prevNumber = 0L;
            // 如果不是第一条数据，记录上一条数据的值
            if (lastNumber.value() != null) {
                prevNumber = lastNumber.value();
            }
            // 更新状态变量为此次的值
            lastNumber.update(in);

            Long ts = 0L;
            if (timerTs.value() != null) {
                ts = timerTs.value();
            }

            if (prevNumber == 0L || in < prevNumber) {
                // 如果时间戳对应的定时器不存在，那么删除操作不执行
                context.timerService().deleteProcessingTimeTimer(ts);
                timerTs.clear(); // 清空存储报警器的时间戳
            } else if (in > prevNumber && ts == 0L) {
                collector.collect(in.toString());
                Long oneSecLater = context.timerService().currentProcessingTime() + 1000L;
                context.timerService().registerProcessingTimeTimer(oneSecLater);
                timerTs.update(oneSecLater); // 状态变量记录定时器的时间戳
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("数字已经连续1s上升了！");
            timerTs.clear();
        }
    }
}

