package com.tutorial.utils.jsonutil;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * TypeEnum jack 的反序列化类
 *
 * @author zhupeiwen
 */
public class TypeEnumJsonDeserializer extends JsonDeserializer<TypeEnum> {
    @Override
    public TypeEnum deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        System.out.println("---===---");
        return Arrays.stream(TypeEnum.values()).filter(typeEnum -> typeEnum.getName().equals(node.asText())).findFirst()
            .get();
    }
}
