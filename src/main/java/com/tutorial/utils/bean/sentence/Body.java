
package com.tutorial.utils.bean.sentence;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhupeiwen
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonPropertyOrder({"sliceId", "msg_id"})
public class Body {

    private String sliceId;
    @JsonProperty(value = "msg_id")
    private String msgId;
    private String msgFrom;
    private String msgToList;
    @JsonFormat(pattern = "yyyy-MM-dd mm:hh:ss")
    private Date msgTime;
    private String msgType;
    private String text;
    private String msgFileUrl;
    private String msgFileSize;
    private String filePlayLength;
    private String fileDealStatus;
    private String staffIds;
    private String staffOneIds;
    private String consumerIds;
    private List<Tag> tag;

}