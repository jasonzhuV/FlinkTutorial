package com.tutorial.bean;

import java.sql.Timestamp;

/**
 * author     : zhupeiwen
 * date       : 2021/11/22 3:11 下午
 * description:
 */
public class UrlCount {
    public String url;
    public Long cnt;
    public Long windowStart;
    public Long windowEnd;

    public UrlCount() {
    }

    public UrlCount(String url, Long cnt, Long windowStart, Long windowEnd) {
        this.url = url;
        this.cnt = cnt;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlCount{" +
                "url='" + url + '\'' +
                ", cnt=" + cnt +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}