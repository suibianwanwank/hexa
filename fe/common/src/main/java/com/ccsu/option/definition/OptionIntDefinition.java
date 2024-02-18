package com.ccsu.option.definition;

import lombok.Getter;

@Getter
public class OptionIntDefinition extends OptionDefinition {

    private Integer defaultValue;

    public OptionIntDefinition(String name, Integer defaultValue) {
        super(name, OptionType.BOOLEAN);
        this.defaultValue = defaultValue;
    }
}
