package com.tutorial.utils.bean.testbean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhupeiwen
 * @date 2021/12/20 5:01 下午
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class Phone implements Contents {
    private String brand;
    private String price;
    private String color;
}
