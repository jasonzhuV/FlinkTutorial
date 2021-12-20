
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
public class Slices {

    private String sliceId;
    private String msgFrom;
    private String msgToList;
    private Date msgTime;
    private String msgType;
    private String text;
    private String msgFileUrl;
    private String msgFileSize;
    private String filePlayLength;
    private String fileDealStatus;
    private List<Tag> tag;

}