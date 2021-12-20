
package com.tutorial.utils.bean.sentence;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Tag {

    private String ruleId;
    private List<Tags> tags;
    private List<Keywords> keywords;

}