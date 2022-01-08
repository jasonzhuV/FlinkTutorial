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
    /**
     * json的数组中的元素的类型可能不一样 这里用Content表示数组中的元素，数组中不同的元素类型继承这个类，表示是数组中的元素 自定义jackson的反序列化器对数组中不同类型的元素进行反序列化
     */
    @JsonDeserialize(using = CustomDeserializer.class)
    private List<Contents> contents;
}
