
package com.tutorial.utils.bean.sentence;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Keywords {

    private int startIndex;
    private int length;
    private String wordGroup;
    private String keyword;

}