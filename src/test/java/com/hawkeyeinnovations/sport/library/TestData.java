package com.hawkeyeinnovations.sport.library;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.io.InputStream;

@UtilityClass
public class TestData {

    public static <T> T load(String path, Class<T> ref) throws IOException {
        try (InputStream inputStream = TestData.class.getResourceAsStream(path)) {
            return ConfiguredObjectMapper.JSON_MAPPER.readValue(inputStream, ref);
        }
    }

    public static <T> T load(String path, TypeReference<T> ref) throws IOException {
        try (InputStream inputStream = TestData.class.getResourceAsStream(path)) {
            return ConfiguredObjectMapper.JSON_MAPPER.readValue(inputStream, ref);
        }
    }
}
