
package com.tutorial.utils.bean.winandslice;

import java.util.Date;
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
public class WindowAndSlices {

    private String msgRoomId;
    private String dialogueId;
    private String brandId;
    private String brandName;
    private String dataVersion;
    private Date day;
    private String startSliceId;
    private String endSliceId;
    private String isRelevant;
    private String sliceNum;
    private String staffIds;
    private String staffOneIds;
    private String consumerIds;
    private String staffNames;
    private List<Details> details;

}