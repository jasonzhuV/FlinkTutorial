package com.tutorial.utils;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 * @date 2021/12/3 2:36 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JsonBean {
    private String id;
    private String version;
    private List<String> data;
}
