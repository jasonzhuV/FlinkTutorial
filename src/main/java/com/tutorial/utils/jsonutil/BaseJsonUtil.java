package com.tutorial.utils.jsonutil;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhupeiwen
 * @date 2021/12/2 7:50 下午
 */
@Slf4j
public abstract class BaseJsonUtil {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(TypeEnum.class, new TypeEnumJsonDeserializer());
        MAPPER.registerModule(module);
    }

    /**
     * Serialize an object to Json String
     * 
     * @param o
     *            object
     * @return Json String
     */
    public static String toJsonStr(Object o) {
        try {
            return MAPPER.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            log.error("json format failed, msg:{}, error:{}" + o.toString() + e.getMessage());
            return null;
        }
    }

    public static <T> Optional<T> parse(String s, TypeReference<T> typeReference) throws IOException {
        try {
            return Optional.of(MAPPER.readValue(s, typeReference));
        } catch (JsonProcessingException e) {
            log.error("json parse failed, msg:{}, error:{}" + s + e.getMessage());
            return Optional.empty();
        }
    }

    public static <T> Optional<T> parse(String s, Class<T> clazz) throws IOException {
        try {
            return Optional.of(MAPPER.readValue(s, clazz));
        } catch (JsonProcessingException e) {
            log.error("json parse failed, msg:{}, error:{}" + s + e.getMessage());
            return Optional.empty();
        }
    }

    public static <T> Optional<T> parse(JsonParser jsonParser, Class<T> clazz) throws IOException {
        try {
            return Optional.of(MAPPER.readValue(jsonParser, clazz));
        } catch (JsonProcessingException e) {
            log.error("json parse failed, msg:{}, error:{}" + jsonParser.toString() + e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 
     * @param jsonParser
     *            used for custom deserializer
     * @param typeReference
     *            type of elements
     * @param <T>
     *            Generic
     * @return Bean
     * @throws IOException
     *             readValue of ObjectMapper
     */
    public static <T> Optional<T> parse(JsonParser jsonParser, TypeReference<T> typeReference) throws IOException {
        try {
            return Optional.of(MAPPER.readValue(jsonParser, typeReference));
        } catch (JsonProcessingException e) {
            log.error("json parse failed, msg:{}, error:{}" + jsonParser.toString() + e.getMessage());
            return Optional.empty();
        }
    }

}
