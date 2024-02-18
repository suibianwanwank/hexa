package com.ccsu.option;

import com.ccsu.option.definition.OptionDefinition;

import java.util.Optional;

public interface OptionManager {

    void register(OptionDefinition optionDefinition);

    void setOption(OptionDefinition key, Object value);

    Optional<Option> getOption(String name);

    String getStringOption(String name);

    Long getLongOption(String name);

    Integer getIntegerOption(String name);

    Boolean getBooleanOption(String name);

    OptionManager getParent();
}

