package com.ccsu.option.manager;

import com.ccsu.option.Option;
import com.ccsu.option.OptionManager;
import com.ccsu.option.definition.OptionDefinition;

import java.util.Optional;

public abstract class AbstractOptionManager implements OptionManager {
    @Override
    public void register(OptionDefinition optionDefinition) {
        getParent().register(optionDefinition);
    }

    @Override
    public void setOption(OptionDefinition key, Object value) {
        getParent().setOption(key, value);
    }

    @Override
    public String getStringOption(String name) {
        Optional<Option> option = getOption(name);
        if (!option.isPresent()) {
            return getParent().getStringOption(name);
        }
        return option.get().getValue();
    }

    @Override
    public Long getLongOption(String name) {
        Optional<Option> option = getOption(name);
        if (!option.isPresent()) {
            return getParent().getLongOption(name);
        }
        return Long.valueOf(option.get().getValue());
    }

    @Override
    public Integer getIntegerOption(String name) {
        Optional<Option> option = getOption(name);
        if (!option.isPresent()) {
            return getParent().getIntegerOption(name);
        }

        return Integer.valueOf(option.get().getValue());
    }

    @Override
    public Boolean getBooleanOption(String name) {
        Optional<Option> option = getOption(name);
        if (!option.isPresent()) {
            return getParent().getBooleanOption(name);
        }
        return Boolean.valueOf(option.get().getValue());
    }
}
