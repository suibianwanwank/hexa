package com.ccsu.option.manager;

import com.ccsu.option.Option;
import com.ccsu.option.OptionStore;
import com.ccsu.option.OptionsClass;
import com.ccsu.option.Scope;
import com.ccsu.option.definition.OptionDefinition;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.StoreManager;
import com.facebook.airlift.log.Logger;
import com.google.inject.Inject;

import java.lang.reflect.Field;

import static com.ccsu.utils.ClassUtils.getClassesByPackage;

/**
 * System level option manager.
 */
public class SystemOptionManager extends StoreBaseOptionManager {

    private static final Logger LOGGER = Logger.get(SystemOptionManager.class);

    @Inject
    public SystemOptionManager(StoreManager storeManager) {
        this(storeManager.getOrCreateDataStore(OptionStore.OPTION_STORE_CONFIG),
                storeManager.getOrCreateDataStore(OptionStore.OPTION_DEFINITION_STORE_CONFIG));
        initAndRegisterAllDefinition();
    }

    public SystemOptionManager(DataStore<String, Option> optionDataStore,
                               DataStore<String, OptionDefinition> optionDefinitionDataStore) {
        super(new OptionStore(optionDataStore, optionDefinitionDataStore), null);
    }

    public void initAndRegisterAllDefinition() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (Class<?> clazz : getClassesByPackage("com.ccsu", classLoader)) {
            OptionsClass options = clazz.getAnnotation(OptionsClass.class);
            if (options == null) {
                continue;
            }
            try {
                for (Field field : clazz.getDeclaredFields()) {
                    OptionDefinition optionDefinition = (OptionDefinition) field.get(null);
                    register(optionDefinition);
                    LOGGER.info("register option %s", optionDefinition.name());
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Scope getScope() {
        return Scope.SYSTEM;
    }
}
