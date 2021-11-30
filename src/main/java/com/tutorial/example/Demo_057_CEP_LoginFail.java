package com.tutorial.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author     : zhupeiwen
 * @date       : 2021/11/26 6:30 下午
 * @description: Complex Event Processing
 * <p>连续三次登陆失败
 */
public class Demo_057_CEP_LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool params = ParameterTool.fromArgs(args);

        System.out.println(params.getProperties());

        SingleOutputStreamOperator<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user-1", "fail", "0.0.0.1", 1000L),
                        new LoginEvent("user-1", "fail", "0.0.0.2", 2000L),
                        new LoginEvent("user-1", "fail", "0.0.0.3", 3000L),
                        new LoginEvent("user-2", "success", "0.0.0.4", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                                        return loginEvent.timestamp;
                                    }
                                })
                );

        // 定义要检测的模板
        // next的意思是严格紧邻
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.eventType);
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.eventType);
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.eventType);
                    }
                });

        // 在流上进行检测
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);

        // 在检测结果中将符合模板的数据提取出来
        SingleOutputStreamOperator<String> result = patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").get(0);
                        LoginEvent second = map.get("second").get(0);
                        LoginEvent third = map.get("third").get(0);
                        return first.userId + " 在ip：" +
                                first.ipAddr + " | " +
                                second.ipAddr + " | " +
                                third.ipAddr + " | " +
                                "登录失败！";
                    }
                });

        result.print();

        env.execute();
    }

    public static class LoginEvent {
        public String userId;
        public String eventType;
        public String ipAddr;
        public Long timestamp;

        public LoginEvent() {
        }

        public LoginEvent(String userId, String eventType, String ipAddr, Long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.ipAddr = ipAddr;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "LoginEvent{" +
                    "userId='" + userId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", ipAddr='" + ipAddr + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
