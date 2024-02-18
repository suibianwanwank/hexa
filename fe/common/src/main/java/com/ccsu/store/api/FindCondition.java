package com.ccsu.store.api;

import lombok.Getter;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

@Getter
public class FindCondition {

    //TODO extend to type exist/in/containAll/and or..
    private final Map<String, FindByRange> conditions;

    private final int limit;

    public FindCondition(Map<String, FindByRange> conditions, int limit) {
        this.conditions = conditions;
        this.limit = limit;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static FindByRange<Long> rangeLong(long start, long end) {
        return new FindByRange<Long>() {
            @Nullable
            @Override
            public Long getStart() {
                return start;
            }

            @Nullable
            @Override
            public Long getEnd() {
                return end;
            }
        };
    }

    public static FindByRange<Integer> rangeInt(Integer start, Integer end) {
        return new FindByRange<Integer>() {
            @Nullable
            @Override
            public Integer getStart() {
                return start;
            }

            @Nullable
            @Override
            public Integer getEnd() {
                return end;
            }
        };
    }

    public static class Builder {

        private Map<String, FindByRange> conditions;

        private int limit;

        private Builder() {
            conditions = new HashMap<>();
        }

        public Builder addCondition(String index, FindByRange condition) {
            conditions.put(index, condition);
            return this;
        }

        public Builder addLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public FindCondition build() {
            return new FindCondition(conditions, limit);
        }
    }
}
