package com.tutorial.utils.bean;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 * @date 2021/12/3 2:51 下午
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue {
    private String key;
    private List<String> values;
}
