package com.ccsu.option.manager;

import com.ccsu.error.CommonException;
import com.ccsu.option.Option;
import com.ccsu.option.OptionManager;
import com.ccsu.option.OptionStore;
import com.ccsu.option.util.OptionUtil;
import com.ccsu.option.definition.OptionDefinition;
import com.ccsu.option.definition.OptionType;

import java.util.Optional;

public class StoreBaseOptionManager extends AbstractOptionManager {

    private final OptionStore optionStore;

    private final OptionManager parent;

    public StoreBaseOptionManager(OptionStore optionStore, OptionManager parent) {
        this.optionStore = optionStore;
        this.parent = parent;
    }

    @Override
    public void register(OptionDefinition optionDefinition) {
        OptionUtil.validateOptionDefinition(optionDefinition);
        //TODO check if type change
        if (!optionStore.containsDefinition(optionDefinition.name())) {
            optionStore.set(optionDefinition);
        }
    }

    @Override
    public void setOption(OptionDefinition key, Object value) {
        if (!checkTypeValid(key.type(), value)) {
            throw new CommonException(null, null);
        }
        Option option = new Option(key.name(), key.type(), value.toString());
        optionStore.set(option);
    }

    @Override
    public Optional<Option> getOption(String name) {
        Optional<OptionDefinition> definitionOptional = optionStore.getDefinition(name);
        if (!definitionOptional.isPresent()) {
            throw new CommonException(null, null);
        }
        return optionStore.getOption(name);
    }

    private Optional<String> checkAndGetValue(String name, OptionType type) {
        Optional<OptionDefinition> definitionOptional = optionStore.getDefinition(name);
        if (!definitionOptional.isPresent()) {
            throw new CommonException(null, null);
        }
        OptionDefinition definition = definitionOptional.get();
        if (definition.type() != type) {
            throw new CommonException(null, null);
        }
        Optional<Option> value = optionStore.getOption(definition.name());
        if (value.isPresent()) {
            return value.map(Option::getValue);
        }
        return Optional.of(definition.getDefaultValue().toString());
    }


    @Override
    public OptionManager getParent() {
        return parent;
    }

    private boolean checkTypeValid(OptionType type, Object value) {
        switch (type) {
            case LONG:
                return value instanceof Long;
            case STRING:
                return value instanceof String;
            case INTEGER:
                return value instanceof Integer;
            case BOOLEAN:
                return value instanceof Boolean;
            default:
                return false;
        }
    }
}
