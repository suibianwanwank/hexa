package com.ccsu.option.manager;

import com.ccsu.option.Option;
import com.ccsu.option.OptionManager;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SessionOptionManager extends AbstractOptionManager {
    private final Map<String, Option> options;
    private final String sessionId;
    private final OptionManager parent;

    public SessionOptionManager(String sessionId,
                                Map<String, Option> options,
                                OptionManager parent) {
        this.options = requireNonNull(options, "options can not be null");
        this.sessionId = requireNonNull(sessionId, "sessionId can not be null");
        this.parent = parent;
    }

    @Override
    public Optional<Option> getOption(String name) {
        Option option = options.get(name);
        if (option == null) {
            return Optional.empty();
        }
        return Optional.of(option);
    }

    @Override
    public OptionManager getParent() {
        return parent;
    }
}
