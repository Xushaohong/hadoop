package com.tencent.mdfs.util;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class JsonUtils {

    private final static ThreadLocal<ObjectMapper> OBJECT_MAPPER_THREAD_LOCAL = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            ObjectMapper om = new ObjectMapper();
            return om;
        }
    };


    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER_THREAD_LOCAL.get().readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String asJson(Object o) {
        try {
            return OBJECT_MAPPER_THREAD_LOCAL.get().writeValueAsString(o);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

}
