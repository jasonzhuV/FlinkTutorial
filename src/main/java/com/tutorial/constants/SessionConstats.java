package com.tutorial.constants;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhupeiwen
 * @date 2022/1/8
 */
public class SessionConstats {
    public final static Map<String, Long> gapConfig = new HashMap<>();

    static {
        gapConfig.put("a", 5 * 1000L);
        gapConfig.put("b", 8 * 1000L);
    }
}
