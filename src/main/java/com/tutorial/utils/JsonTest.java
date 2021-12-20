package com.tutorial.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.tutorial.utils.bean.BccTagMessageBase;
import com.tutorial.utils.bean.testbean.Contents;
import com.tutorial.utils.bean.testbean.People;
import com.tutorial.utils.bean.testbean.Phone;
import com.tutorial.utils.bean.testbean.RootObject;
import com.tutorial.utils.jsonutil.BaseJsonUtil;

/**
 * @author zhupeiwen
 * @date 2021/12/3 2:06 下午
 */
public class JsonTest {

    @Test
    public void t1() throws IOException {
        InputStream jsonInputStream = JsonTest.class.getClassLoader().getResourceAsStream("message.json");
        String jsonStr = "";
        try {
            assert jsonInputStream != null;
            jsonStr = IOUtils.toString(jsonInputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Optional<BccTagMessageBase> parse = BaseJsonUtil.parse(jsonStr, new TypeReference<BccTagMessageBase>() {});

        parse.ifPresent(e -> System.out.println(BaseJsonUtil.toJsonStr(e)));

        if (parse.isPresent()) {
            BccTagMessageBase bccTagMessageBase = parse.get();
        }
    }

    @Test
    public void t2() throws IOException {
        String s =
            "[{\"field1\": \"field1\", \"field2\": \"field2\",  \"field3\": \"field3\"},{  \"field4\": \"field4\",  \"field5\": \"field5\",  \"field6\": \"field6\"}]";
        // 获取内容，根据标记判断对应的bean，再反序列化
        Optional<List<Map<String, Object>>> parse =
            BaseJsonUtil.parse(s, new TypeReference<List<Map<String, Object>>>() {});

        // if (parse.isPresent()) {
        // parse.get().contains()
        // }
    }

    @Test
    public void t3() throws IOException {
        InputStream jsonInputStream = JsonTest.class.getClassLoader().getResourceAsStream("test.json");
        String jsonStr = "";
        try {
            assert jsonInputStream != null;
            jsonStr = IOUtils.toString(jsonInputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Optional<RootObject> parse = BaseJsonUtil.parse(jsonStr, new TypeReference<RootObject>() {});

        if (parse.isPresent()) {
            List<Contents> contents = parse.get().getContents();
            for (Contents content : contents) {
                if (content instanceof People) {
                    System.out.println("people : " + content);
                } else if (content instanceof Phone) {
                    System.out.println("phone : " + content);
                }
            }
        }
    }

    @Test
    public void t4() throws JsonProcessingException {
        JsonNode jsonNode = BaseJsonUtil.MAPPER.readTree("123");
        Map<String, Object> stringObjectMap =
            BaseJsonUtil.MAPPER.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {});
    }
}
