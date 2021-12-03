package com.tutorial.utils.bean;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 * @date 2021/12/3 2:57 下午
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Tag {
    private List<SliceTag> tag;
}
