package com.ccsu.option.definition;

import lombok.Getter;

@Getter
public class OptionLongDefinition extends OptionDefinition {

    private Long defaultValue;

    public OptionLongDefinition(String name, Long defaultValue) {
        super(name, OptionType.LONG);
        this.defaultValue = defaultValue;
    }
}
