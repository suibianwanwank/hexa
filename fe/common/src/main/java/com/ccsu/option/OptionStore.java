package com.ccsu.option;

import com.ccsu.option.definition.OptionDefinition;
import com.ccsu.store.api.Converter;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.Format;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.store.api.StoreConfigBuilder;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OptionStore {
    public static final StoreConfig<String, Option> OPTION_STORE_CONFIG = new StoreConfigBuilder<>(
            "options",
            Converter.STRING_UTF_8,
            Format.ofPojo(Option.class)
    ).build();

    public static final StoreConfig<String, OptionDefinition> OPTION_DEFINITION_STORE_CONFIG = new StoreConfigBuilder<>(
            "option-definitions",
            Converter.STRING_UTF_8,
            Format.ofPojo(OptionDefinition.class)
    ).build();

    private final DataStore<String, Option> optionDataStore;

    private final DataStore<String, OptionDefinition> optionDefinitionDataStore;

    public OptionStore(DataStore<String, Option> optionDataStore,
                       DataStore<String, OptionDefinition> optionDefinitionDataStore) {
        this.optionDataStore = requireNonNull(optionDataStore);
        this.optionDefinitionDataStore = requireNonNull(optionDefinitionDataStore);
    }

    public boolean containsDefinition(String name) {
        return optionDefinitionDataStore.contains(name);
    }

    public void set(OptionDefinition optionDefinition) {
        optionDefinitionDataStore.put(optionDefinition.name(), optionDefinition);
    }

    public Optional<OptionDefinition> getDefinition(String name) {
        return Optional.ofNullable(optionDefinitionDataStore.get(name)).map(EntityWithTag::getValue);
    }

    public void set(Option option) {
        optionDataStore.put(option.getKey(), option);
    }

    public void delete(String key) {
        optionDataStore.delete(key);
    }

    public Optional<Option> getOption(String key) {
        return Optional.ofNullable(optionDataStore.get(key)).map(EntityWithTag::getValue);
    }
}
