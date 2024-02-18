package com.ccsu.module;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class JacksonModule extends AbstractModule {

    /**
     * Provide Jackson json serialisation configuration
     */
    @Provides
    public ObjectMapper proviceObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        // set Jackson visibility rules
        objectMapper.setVisibility(VisibilityChecker.Std.defaultInstance()
                .withFieldVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
                .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
                .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
        return objectMapper;
    }
}
