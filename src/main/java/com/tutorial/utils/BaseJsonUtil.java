package com.tutorial.utils;

import java.io.IOException;
import java.util.Optional;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * @author zhupeiwen
 * @date 2021/12/2 7:50 下午
 */
public abstract class BaseJsonUtil {
    private static final Logger logger = LogManager.getLogger(BaseJsonUtil.class.getName());
    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
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
            logger.error("json format failed, msg:{}, error:{}" + o.toString() + e.getMessage());
            return null;
        }
    }

    public static <T> Optional<T> parse(String s, TypeReference<T> typeReference) throws IOException {
        try {
            return Optional.of(MAPPER.readValue(s, typeReference));
        } catch (JsonProcessingException e) {
            logger.error("json parse failed, msg:{}, error:{}" + s + e.getMessage());
            return Optional.empty();
        }
    }

    public static <T> Optional<T> parse(String s, Class<T> clazz) throws IOException {
        try {
            return Optional.of(MAPPER.readValue(s, clazz));
        } catch (JsonProcessingException e) {
            logger.error("json parse failed, msg:{}, error:{}" + s + e.getMessage());
            return Optional.empty();
        }
    }

}
