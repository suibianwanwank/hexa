package com.ccsu.option.util;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.option.definition.OptionDefinition;
import com.ccsu.option.definition.OptionType;
import org.apache.bval.util.StringUtils;

public class OptionUtil {
    private OptionUtil() {
    }

    /**
     * Verify that the option definition is legal.
     */
    public static void validateOptionDefinition(OptionDefinition definition) {
        if (StringUtils.isBlank(definition.name())) {
            throw new CommonException(CommonErrorCode.SYSTEM_OPTION_ERROR,
                    "definition name can not be empty");
        }
        Object defaultValue = definition.getDefaultValue();
        if (defaultValue == null) {
            throw new CommonException(CommonErrorCode.SYSTEM_OPTION_ERROR,
                    "Default value can not be null");
        }
        validateValueType(defaultValue.toString(), definition.type());
    }

    /**
     * Checks whether the data can be converted to option type.
     */
    public static void validateValueType(String value, OptionType type) {
        switch (type) {
            case LONG:
                try {
                    Long.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("long option value %s can not format", value), e);
                }
                break;
            case DOUBLE:
                try {
                    Double.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("double option value %s can not format", value), e);
                }
                break;
            case INTEGER:
                try {
                    Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("int option value %s can not format", value), e);
                }
                break;
            case BOOLEAN:
                if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                    throw new IllegalArgumentException(
                            String.format("boolean option value should true or false %s", value));
                }
                break;
            case STRING:
                break;
            default:
                throw new CommonException(null, null);
        }
    }
}
