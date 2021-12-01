package com.tutorial.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.FileOutputStream;
import java.util.Random;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/23 3:03 下午
 * <p>description: 自定义 Sink
 */
public class Demo055SinkCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new RichSourceFunction<String>() {
                    private Random random = new Random();
                    private boolean running = true;
                    private String[] arr = {"a","b","c"};
                    @Override
                    public void run(SourceContext<String> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(arr[random.nextInt(arr.length)]);
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(e -> 1) // 在流里用了状态变量，这是键控状态的 (ValueState 是键控状态)
                .addSink(new RichSinkFunction<String>() {
                    private ValueState<Long> count;
                    private FileOutputStream outfile;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        outfile = new FileOutputStream("src/main/resources/out.txt");
                        count = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("counter", Types.LONG)
                        );
                    }

                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        if (count.value() != null) {
                            count.update(count.value() + 1L);
                        } else {
                            count.update(1L);
                        }
                        System.out.println("第 " + count.value() + " 条数据");
                        outfile.write(value.getBytes());
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        outfile.close();
                    }
                });

        env.execute();
    }
}
