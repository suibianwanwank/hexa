package com.ccsu.option.definition;

public class OptionDefinition {

    private final String name;

    private final OptionType type;

    public OptionDefinition(String name, OptionType type) {
        this.name = name;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public OptionType type() {
        return type;
    }

    public Object getDefaultValue() {
        return null;
    }
}
