package com.ccsu.option.definition;

import lombok.Getter;

@Getter
public class OptionBooleanDefinition extends OptionDefinition {

    private final Boolean defaultValue;

    public OptionBooleanDefinition(String name, boolean defaultValue) {
        super(name, OptionType.BOOLEAN);
        this.defaultValue = defaultValue;
    }
}
