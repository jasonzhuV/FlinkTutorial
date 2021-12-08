package com.tutorial.utils.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 * @date 2021/12/3 4:27 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
// @JsonIgnoreProperties(ignoreUnknown = true)
public class TestFieldName {
    @JsonProperty("ruleId")
    private String ruleId;

    private String sliceNum;
}
