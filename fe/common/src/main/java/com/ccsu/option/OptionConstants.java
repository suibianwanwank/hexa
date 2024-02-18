package com.ccsu.option;

import com.ccsu.option.definition.OptionDefinition;
import com.ccsu.option.definition.OptionLongDefinition;

public final class OptionConstants {
    private OptionConstants() {
    }

    public static final OptionDefinition PLANNER_TIME_OUT_MS = new OptionLongDefinition("planner.timeout.ms", -1L);

}
