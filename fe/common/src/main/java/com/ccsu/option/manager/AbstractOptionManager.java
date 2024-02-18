package com.ccsu.option.manager;

import com.ccsu.option.Option;
import com.ccsu.option.OptionManager;
import com.ccsu.option.definition.OptionDefinition;

import java.util.Optional;

public abstract class AbstractOptionManager implements OptionManager {

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
        return option.map(value -> Long.valueOf(value.getValue())).orElseGet(() -> getParent().getLongOption(name));
    }

    @Override
    public Integer getIntegerOption(String name) {
        Optional<Option> option = getOption(name);
        return option.map(value -> Integer.valueOf(value.getValue())).orElseGet(() -> getParent().getIntegerOption(name));

    }

    @Override
    public Boolean getBooleanOption(String name) {
        Optional<Option> option = getOption(name);
        return option.map(value -> Boolean.valueOf(value.getValue())).orElseGet(() -> getParent().getBooleanOption(name));
    }
}
