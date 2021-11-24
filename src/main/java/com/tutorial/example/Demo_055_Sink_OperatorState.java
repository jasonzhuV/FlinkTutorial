package com.tutorial.example;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Spliterator;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/23 3:03 下午
 * <p>description: 自定义 Sink
 */
public class Demo_055_Sink_OperatorState {
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
                .addSink(new MySinkWithOperatorState(3)); // operator state 不能直接从getRunTimeContext 获取，需要用下面的方式

        env.execute();
    }

    public static class MySinkWithOperatorState implements SinkFunction<String>, CheckpointedFunction {
        private final int threshold;

        private transient ListState<String> checkpointedState;

        private List<String> bufferedElements;

        private ListState<Long> counter;

        private FileOutputStream outfile;

        public MySinkWithOperatorState(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            outfile.write(value.getBytes());

            if (counter.get().spliterator().getExactSizeIfKnown() == 0) {
                counter.add(1L);
            } else {
                Long last = counter.get().iterator().next();
                counter.clear();
                counter.add(last + 1L);
            }
            System.out.println("第 " + counter.get().iterator().next() + " 条数据");
//            bufferedElements.add(value);
//            if (bufferedElements.size() == threshold) {
//                for (String element: bufferedElements) {
//                    System.out.println(element);
//                    outfile.write(element.getBytes());
//                }
//                bufferedElements.clear();
//            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (String element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            outfile = new FileOutputStream("src/main/resources/out.txt");

            OperatorStateStore operatorStateStore = context.getOperatorStateStore();

            checkpointedState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<String>("sink with operatorState", Types.STRING)
            );

            counter = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>("counter", Types.LONG)
            );

            if (context.isRestored()) {
                for (String element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
