package com.tutorial.utils.jsonutil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.tutorial.utils.bean.testbean.Contents;
import com.tutorial.utils.bean.testbean.People;
import com.tutorial.utils.bean.testbean.Phone;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhupeiwen
 * @date 2021/12/20 5:05 下午
 */
@Slf4j
public class CustomDeserializer extends JsonDeserializer<List<Contents>> {

    @Override
    public List<Contents> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
        ArrayList<Contents> contents = new ArrayList<>();

        Optional<List<Map<String, Object>>> objList =
            BaseJsonUtil.parse(jsonParser, new TypeReference<List<Map<String, Object>>>() {});

        if (objList.isPresent()) {
            for (Map<String, Object> stringObjectMap : objList.get()) {
                if (stringObjectMap.containsKey("name")) {
                    Optional<People> peopleOptional = BaseJsonUtil
                        .parse(BaseJsonUtil.MAPPER.writeValueAsString(stringObjectMap), new TypeReference<People>() {});
                    peopleOptional.ifPresent(people -> contents.add(people));
                } else {
                    Optional<Phone> phoneOptional = BaseJsonUtil
                        .parse(BaseJsonUtil.MAPPER.writeValueAsString(stringObjectMap), new TypeReference<Phone>() {});
                    phoneOptional.ifPresent(phone -> contents.add(phone));
                }
            }
        }
        return contents;
    }
}
