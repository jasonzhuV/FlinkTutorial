package com.tutorial.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  * @author     : zhupeiwen
 */
public class Demo003AnonymousMethod {
    /**
     * 匿名函数写法（当有类型擦除的时候，需要指定返回类型），java的类型推断的能力弱
     * java的话，推荐使用匿名内部类
     */
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello world", "hello flink")
                .flatMap((String s, Collector<Tuple2<String, Long>> out) ->{
                    String[] arr = s.split(" ");
                    for (String e : arr) {
                        out.collect(Tuple2.of(e, 1L));
                    }
                })
                //
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        env.execute();

    }
}
