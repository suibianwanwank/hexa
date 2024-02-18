package com.ccsu.option.definition;

import lombok.Getter;

@Getter
public class OptionStringDefinition extends OptionDefinition {

    private final String defaultValue;

    public OptionStringDefinition(String name, String defaultValue) {
        super(name, OptionType.STRING);
        this.defaultValue = defaultValue;
    }
}
