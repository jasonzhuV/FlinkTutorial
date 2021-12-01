package com.tutorial.example;

import com.tutorial.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/23 3:03 下午
 * <p>description: 写入Redis，幂等性的方式
 */
public class Demo053SinkRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

        env
                .readTextFile("/Users/jason/MyProjects/FlinkTutorial/src/main/resources/clicks.csv")
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return Event.of(arr[0], arr[1], Long.parseLong(arr[2]) * 1000L);
                    }
                })
                .addSink(new RedisSink<Event>(conf, new MyRedisSink()));

        env.execute();
    }

    public static class MyRedisSink implements RedisMapper<Event> {
        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.url;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }
    }
}
