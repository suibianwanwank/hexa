package com.ccsu.option;

import com.ccsu.option.definition.OptionType;

import java.util.Objects;

public class Option {
    private String key;

    private String name;

    private OptionType type;

    private String value;

    private Scope scope;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public OptionType getType() {
        return type;
    }

    public void setType(OptionType type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }
    
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Option() {
    }

    public Option(String name, OptionType type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Option option = (Option) o;
        return Objects.equals(name, option.name)
                && type == option.type
                && Objects.equals(value, option.value)
                && scope == option.scope
                && Objects.equals(key, option.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, value, scope, key);
    }

    @Override
    public String toString() {
        return "Option{"
                + "name='" + name + '\''
                + ", type=" + type
                + ", value='" + value + '\''
                + ", scope=" + scope
                + ", key='" + key + '\''
                + '}';
    }
}
