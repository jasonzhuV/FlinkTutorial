package com.tutorial.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tutorial.utils.bean.Tag;
import com.tutorial.utils.bean.TestFieldName;
import com.tutorial.utils.pojo.BccSliceTagMessage;

/**
 * @author zhupeiwen
 * @date 2021/12/3 2:06 下午
 */
public class JsonTest {
    @Test
    public void test1() throws IOException {
        String s =
            "{\"tag\":[{\"ruleId\":\"0001\",\"tags\":[{\"key\":\"风险\",\"values\":[\"产品负面反馈\"]},{\"key\":\"风险类型\",\"values\":[\"信誉风险\"]}]},{\"ruleId\":\"0002\",\"tags\":[{\"key\":\"事件\",\"values\":[\"产品介绍\",\"竞品\",\"询价\"]}]},{\"ruleId\":\"0003\",\"tags\":[{\"key\":\"竞品\",\"values\":[\"竞品1\",\"竞品2\"]}]}]}";
        Optional<Map<String, Object>> parse = BaseJsonUtil.parse(s, new TypeReference<Map<String, Object>>() {});

        Map<String, Object> soMap = parse.get();

        for (Map.Entry<String, Object> next : soMap.entrySet()) {
            System.out.println(next.getKey());
        }
    }

    @Test
    public void test2() {
        List<String> data = Arrays.asList("testing data1", "testing data2");
        JsonBean test = new JsonBean("1001", "0.0.1", data);
        System.out.println(BaseJsonUtil.toJsonStr(test));
    }

    @Test
    public void test3() throws IOException {
        String s =
            "{\"tag\":[{\"ruleId\":\"0001\",\"tags\":[{\"key\":\"风险\",\"values\":[\"产品负面反馈\"]},{\"key\":\"风险类型\",\"values\":[\"信誉风险\"]}]},{\"ruleId\":\"0002\",\"tags\":[{\"key\":\"事件\",\"values\":[\"产品介绍\",\"竞品\",\"询价\"]}]},{\"ruleId\":\"0003\",\"tags\":[{\"key\":\"竞品\",\"values\":[\"竞品1\",\"竞品2\"]}]}]}";
        Optional<Tag> parse = BaseJsonUtil.parse(s, new TypeReference<Tag>() {});

        System.out.println(BaseJsonUtil.toJsonStr(parse.get()));

    }

    @Test
    public void test4() throws IOException {
        String s = "{\"slice_num\": \"2\",\"ruleId\": \"0001\"}";
        Optional<TestFieldName> parse = BaseJsonUtil.parse(s, new TypeReference<TestFieldName>() {});

        System.out.println(BaseJsonUtil.toJsonStr(parse.get()));
    }

    @Test
    public void test5() throws IOException {
        String s =
            "{\"code\":\"\",\"body\":{\"msg_room_id\":\"wrzuQBCQAAr2GkXs2Jl7doygJswKKmlA\",\"brand_id\":\"\",\"brand_name\":\"\",\"data_version\":\"\",\"day\":\"2021-11-11\",\"dialogue_id\":\"1100001\",\"start_slice_id\":\"00001\",\"end_slice_id\":\"00002\",\"is_relevant\":\"1\",\"slice_num\":\"2\",\"staff_ids\":\"Wxmaapekddlqkf\",\"consumer_ids\":\"ZHANGsan\",\"staff_names\":\"张三\",\"keyword\":[\"帮到您？\",\"藿香正气水\"],\"tag\":[{\"ruleId\":\"00011\",\"tags\":[{\"key\":\"话题\",\"values\":[\"需求问询\"]}]}],\"slices\":[{\"sliceId\":\"mdxalffa001\",\"msg_from\":\"0001\",\"msg_to_list\":\"0002\",\"msg_time\":\"2021-11-11 12:00:00\",\"msg_type\":\"text\",\"text\":\"请问有什么可以帮到您\",\"msg_file_url\":\"\",\"msg_file_size\":\"\",\"file_play_length\":\"\",\"file_deal_status\":\"\",\"keyword\":[\"请问\",\"帮到您\"],\"tag\":[{\"ruleId\":\"0001\",\"tags\":[{\"key\":\"话题\",\"values\":[\"迎宾\"]},{\"key\":\"主题\",\"values\":[\"需求问询\"]}]}]},{\"sliceId\":\"mdxalffa002\",\"msg_from\":\"0002\",\"msg_to_list\":\"0001\",\"msg_time\":\"2021-11-11 12:00:00\",\"msg_type\":\"text\",\"text\":\"有没有藿香正气水？\",\"msg_file_url\":\"\",\"msg_file_size\":\"\",\"file_play_length\":\"\",\"file_deal_status\":\"\",\"keyword\":[\"藿香正气水\"],\"tag\":[{\"ruleId\":\"0002\",\"tags\":[{\"key\":\"主题\",\"values\":[\"产品体积\"]}]}]}]},\"currentNode\":\"\",\"effectTime\":\"\",\"brandCode\":\"\",\"cloudType\":\"\",\"engineId\":\"\",\"level\":\"\",\"fileKeyList\":{\"url\":\"http://file.com/xxx.pdf\"},\"nodeParam\":\"\"}";
        Optional<BccSliceTagMessage> parse = BaseJsonUtil.parse(s, new TypeReference<BccSliceTagMessage>() {});
        parse.ifPresent(bccSliceTagMessage -> System.out.println());

    }
}
