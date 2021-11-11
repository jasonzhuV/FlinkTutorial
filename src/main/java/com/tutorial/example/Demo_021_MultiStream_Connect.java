package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import com.tutorial.source.UrlSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 分流 keyBy （或者物理分区）
 * 合流，将两条流合并在一起
 * connect 算子
 *      ：将两条流合并在一起处理
 *      按照FIFO的方式
 *      与union算子的区别：只能合并两条流，但是两条流中的数据类型可以不一样
 * 处理方式一般
 *      1、一条流keyBy一条流广播
 *      2、两条流都keyBy
 */
public class Demo_021_MultiStream_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clickSource = env.addSource(new ClickSource());
        DataStreamSource<String> urlSource = env.addSource(new UrlSource());

        clickSource
                .keyBy(r -> r.user)
                .connect(urlSource.broadcast())
                .flatMap(new CoFlatMapFunction<Event, String, Event>() {
                    String url = "";
                    @Override
                    public void flatMap1(Event event, Collector<Event> collector) throws Exception {
                        // 处理第一条流的数据
                        if (event.url.equals(url)) {
                            collector.collect(event);
                        }
                    }

                    @Override
                    public void flatMap2(String value, Collector<Event> collector) throws Exception {
                        // 处理第二条流的数据
                        url = value;
                    }
                })
                .print();

        env.execute();
    }

}
