package com.tutorial.utils.bean;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 * @date 2021/12/3 6:54 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SliceTag {
    private String ruleId;
    private List<KeyValue> tags;
}
