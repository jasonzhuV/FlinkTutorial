package com.tutorial.generic;

import com.tutorial.bean.Event;

public class Test {
    public static void main(String[] args) {
        MyDataStream.<Event>getStreamInstance().assignTimestampsAndWatermarks(
                MyWatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new MySerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return 0;
                            }
                        })
        );
    }
}
