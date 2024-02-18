/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ccsu.store.api;

import com.ccsu.datastore.api.SearchTypes;
import com.google.common.base.Preconditions;

/**
 * define the field how to store and query in index store
 *
 * @see DataIndexStore
 * @see IndexConverter
 */
public class IndexKey {
    public static final String LOWER_CASE_SUFFIX = "_LC";

    private final String indexFieldName;
    private final Class<?> valueType;
    private final boolean canContainMultipleValues;
    private final SearchTypes.SearchFieldSorting.FieldType sortedValueType;
    private final boolean stored;

    public IndexKey(String indexFieldName,
                    Class<?> valueType,
                    boolean canContainMultipleValues,
                    SearchTypes.SearchFieldSorting.FieldType sortedValueType,
                    boolean stored) {
        this.indexFieldName = indexFieldName;
        this.valueType = valueType;
        this.canContainMultipleValues = canContainMultipleValues;
        this.sortedValueType = sortedValueType;
        this.stored = stored;
    }

    @Override
    public String toString() {
        return indexFieldName;
    }

    public String getIndexFieldName() {
        return indexFieldName;
    }

    public boolean isStored() {
        return stored;
    }

    public boolean isSorted() {
        return sortedValueType != null;
    }

    public SearchTypes.SearchFieldSorting.FieldType getSortedValueType() {
        return sortedValueType;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    public boolean canContainMultipleValues() {
        return canContainMultipleValues;
    }

    public SearchTypes.SearchFieldSorting toSortField(SearchTypes.SearchFieldSorting.SortOrder order) {
        Preconditions.checkArgument(isSorted());
        return SearchTypes.SearchFieldSorting.newBuilder()
                .setField(indexFieldName)
                .setType(sortedValueType)
                .setOrder(order)
                .build();
    }

    public static Builder newBuilder(String indexFieldName, Class<?> valueType) {
        Preconditions.checkArgument(indexFieldName != null, "IndexKey requires a field name");
        Preconditions.checkArgument(valueType != null, "IndexKey requires a value type");

        return new Builder(indexFieldName, valueType);
    }

    /**
     * IndexKey Builder
     */
    public static class Builder {
        private final String indexFieldName;
        private final Class<?> valueType;
        private boolean canContainMultipleValues;
        private SearchTypes.SearchFieldSorting.FieldType sortedValueType;
        private boolean stored;

        Builder(String indexFieldName, Class<?> valueType) {
            this.indexFieldName = indexFieldName;
            this.valueType = valueType;

            if (Integer.class.equals(valueType)) {
                setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.INTEGER);
            } else if (Double.class.equals(valueType)) {
                setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.DOUBLE);
            } else if (Long.class.equals(valueType)) {
                setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG);
            } else if (String.class.equals(valueType)) {
                setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING);
            }
        }

        public Builder setSortedValueType(SearchTypes.SearchFieldSorting.FieldType sortedValueType) {
            this.sortedValueType = sortedValueType;
            return this;
        }

        public Builder setStored(boolean stored) {
            this.stored = stored;
            return this;
        }

        public Builder setCanContainMultipleValues(Boolean canContainMultipleValues) {
            this.canContainMultipleValues = canContainMultipleValues;
            return this;
        }

        public IndexKey build() {
            return new IndexKey(indexFieldName, valueType, canContainMultipleValues, sortedValueType, stored);
        }
    }
}
