package com.tutorial.utils.bean.testbean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhupeiwen
 * @date 2021/12/20 5:00 下午
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class People extends Contents {
    private String name;
    private String age;
    private String gender;
}
