
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
public class Tags {

    private String key;
    private String gcode;
    private List<String> values;
    private List<String> codes;

}