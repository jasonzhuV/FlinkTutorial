package com.tutorial.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * author     : zhupeiwen
 * date       : 2021/11/17 11:14 上午
 * description: 实时对账，理解 CoProcessFunction
 * CoProcessFunction 两条流都是经过keyBy的，如果一条是keyBy的一条是广播的，使用BroadcastProcessFunction
 */
public class Demo043CoProcessFunctionCheck {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // app内的支付 第一个字段是订单ID
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env
                .fromElements(
                        Tuple3.of("order-1", "app", 1000L),
                        Tuple3.of("order-2", "app", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> in, long l) {
                                        return in.f2;
                                    }
                                })
                );
        // 微信上是否收到支付
        SingleOutputStreamOperator<Tuple3<String, String, Long>> weixinStream = env
                .fromElements(
                        Tuple3.of("order-1", "weixin", 3000L),
                        Tuple3.of("order-3", "weixin", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> in, long l) {
                                        return in.f2;
                                    }
                                })
                );

        appStream
                .keyBy(r -> r.f0)
                .connect(weixinStream.keyBy(r -> r.f0))
                .process(new Match())
                .print();

        env.execute();
    }

    public static class Match extends CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String> {
        private ValueState<Tuple3<String, String, Long>> appEvent;
        private ValueState<Tuple3<String, String, Long>> weixinEvent;
        // 初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            appEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app", Types.TUPLE(
                            Types.STRING, Types.STRING, Types.LONG
                    ))
            );
            weixinEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("weixin", Types.TUPLE(
                            Types.STRING, Types.STRING, Types.LONG
                    ))
            );
        }

        // app的支付流
        @Override
        public void processElement1(Tuple3<String, String, Long> app, Context context, Collector<String> collector) throws Exception {
            if (weixinEvent.value() != null) {
                collector.collect(app.f0 + " 对账成功");
                weixinEvent.clear();
            } else {
                appEvent.update(app);
                context.timerService().registerEventTimeTimer(app.f2 + 5000L);
            }
        }

        // 微信的支付流
        @Override
        public void processElement2(Tuple3<String, String, Long> weixin, Context context, Collector<String> collector) throws Exception {
            if (appEvent.value() != null) {
                collector.collect(weixin.f0 + " 对账成功");
                appEvent.clear();
            } else {
                weixinEvent.update(weixin);
                context.timerService().registerEventTimeTimer(weixin.f2 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (appEvent.value() != null) {
                out.collect(appEvent.value().f0 + " 对账失败，订单的weixin支付信息未到");
                appEvent.clear();
            }
            if (weixinEvent.value() != null) {
                out.collect(weixinEvent.value().f0 + " 对账失败，订单的app支付信息未到");
                weixinEvent.clear();
            }
        }
    }
}
