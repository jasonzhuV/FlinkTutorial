package com.tutorial.wm;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

/**
 * @author zhupeiwen
 * @date 2022/1/8
 */
public class MyWatermarkStrategy<T> implements WatermarkStrategy<T> {
    private static final long serialVersionUID = 1L;
    private final AssignerWithPeriodicWatermarks<T> wms;
    private final Long autoWatermarkInterval;

    public MyWatermarkStrategy(AssignerWithPeriodicWatermarks<T> wms, Long autoWatermarkInterval) {
        this.wms = (AssignerWithPeriodicWatermarks)Preconditions.checkNotNull(wms);
        this.autoWatermarkInterval = autoWatermarkInterval;
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return this.wms;
    }

    @Override
    public WatermarkGenerator<T>
        createWatermarkGenerator(org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
        return new MyAssignerWithPeriodicWatermarksAdapter(this.wms, this.autoWatermarkInterval);
    }

    public static class MyAssignerWithPeriodicWatermarksAdapter<T> implements WatermarkGenerator<T> {
        private final AssignerWithPeriodicWatermarks<T> wms;
        private final Long autoWatermarkInterval;

        public MyAssignerWithPeriodicWatermarksAdapter(AssignerWithPeriodicWatermarks<T> wms,
            Long autoWatermarkInterval) {
            this.wms = (AssignerWithPeriodicWatermarks)Preconditions.checkNotNull(wms);
            this.autoWatermarkInterval = autoWatermarkInterval;
        }

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {}

        @Override
        public void onPeriodicEmit(org.apache.flink.api.common.eventtime.WatermarkOutput output) {
            org.apache.flink.streaming.api.watermark.Watermark next = this.wms.getCurrentWatermark();
            if (next != null) {
                Long emitWatermarkTimestamp = (autoWatermarkInterval != null && autoWatermarkInterval.longValue() != 0L
                    && (System.currentTimeMillis() - next.getTimestamp() < autoWatermarkInterval)) ? next.getTimestamp()
                        : System.currentTimeMillis();
                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(emitWatermarkTimestamp));
            }
        }
    }
}
