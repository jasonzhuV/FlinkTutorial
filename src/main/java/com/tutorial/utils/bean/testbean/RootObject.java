package com.tutorial.utils.bean.testbean;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.tutorial.utils.jsonutil.CustomDeserializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 * @date 2021/12/20 4:59 下午
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RootObject {
    private String id;
    @JsonDeserialize(using = CustomDeserializer.class)
    private List<Contents> contents;
}
