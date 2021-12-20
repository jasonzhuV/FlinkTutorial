
package com.tutorial.utils.bean;

import com.tutorial.utils.bean.sentence.Body;
import com.tutorial.utils.bean.sentence.FileKeyList;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class BccTagMessageBase {

    private String code;
    private Body body;
    private String currentNode;
    private String effectTime;
    private String brandCode;
    private String cloudType;
    private String engineId;
    private String level;
    private FileKeyList fileKeyList;
    private String nodeParam;

}