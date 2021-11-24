package com.tutorial.example;

import com.tutorial.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/23 3:03 下午
 * <p>description: 写入 MySQL
 * <p>MySQL的连接器 flink1.13才有，下游不常向MySQL写数据
 */
public class Demo_054_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(
                        new SensorReading("sensor_1", 1.1, 1000L),
                        new SensorReading("sensor_2", 2.2, 2000L),
                        new SensorReading("sensor_3", 3.3, 3000L)
                )
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO temps (id, temp) VALUES (?, ?)",
                                (statement, r) -> {
                                    statement.setString(1, r.id);
                                    statement.setDouble(2, r.temperature);
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:mysql://localhost:3306/sensor")
                                        // 使用mysql 5.7的话，没有cj
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername("root")
                                        .withPassword("123456")
                                        .build()
                        )
                );

        env.execute();
    }

    public static class SensorReading {
        public String id;
        public Double temperature;
        public Long timestamp;

        public SensorReading() {
        }

        public SensorReading(String id, Double temperature, Long timestamp) {
            this.id = id;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "id='" + id + '\'' +
                    ", temperature=" + temperature +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
