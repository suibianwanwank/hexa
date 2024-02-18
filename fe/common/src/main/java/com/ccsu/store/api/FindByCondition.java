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
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.List;

/**
 * define how to query index
 */
@Value.Immutable
public interface FindByCondition {
    int DEFAULT_PAGE_SIZE = 5000;
    int DEFAULT_OFFSET = 0;
    int DEFAULT_LIMIT = Integer.MAX_VALUE;

    /**
     * Retrieves search condition.
     *
     * @return condition.
     */
    @Nullable
    SearchTypes.SearchQuery getCondition();

    /**
     * Retrieves sort.
     *
     * @return sort.
     */
    List<SearchTypes.SearchFieldSorting> getSort();

    /**
     * Retrieves page size.
     *
     * @return page size.
     */
    @Value.Default
    default int getPageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieves offset.
     *
     * @return offset.
     */
    @Value.Default
    default int getOffset() {
        return DEFAULT_OFFSET;
    }

    /**
     * Retrieves limit.
     *
     * @return limit
     */
    @Value.Default
    default int getLimit() {
        return DEFAULT_LIMIT;
    }

    static SearchTypes.SearchQuery term(String field,
                                        String value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.TERM)
                .setTerm(SearchTypes.SearchQuery.Term.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery term(IndexKey field,
                                        String value) {
        return term(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery ne(IndexKey field,
                                      String value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.NOT_EQUAL)
                .setNotEqual(SearchTypes.SearchQuery.NotEqual.newBuilder()
                        .setField(field.getIndexFieldName()).
                        setValue(value))
                .build();
    }

    static SearchTypes.SearchQuery ne(String fieldName,
                                      String value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.NOT_EQUAL)
                .setNotEqual(SearchTypes.SearchQuery.NotEqual.newBuilder().setField(fieldName).setValue(value)).build();
    }

    static SearchTypes.SearchQuery bool(SearchTypes.SearchQuery.BooleanOp op,
                                        Iterable<SearchTypes.SearchQuery> searchQueries) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.BOOLEAN)
                .setBoolean(SearchTypes.SearchQuery.Boolean.newBuilder().setOp(op).addAllClauses(searchQueries))
                .build();
    }

    static SearchTypes.SearchQuery rangeLong(String field,
                                             long min,
                                             boolean minInclusive,
                                             long max,
                                             boolean maxInclusive) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.RANGE_LONG)
                .setRangeLong(SearchTypes.SearchQuery.RangeLong.newBuilder().setField(field)
                        .setMin(min).setMinInclusive(minInclusive).setMax(max).setMaxInclusive(maxInclusive)).build();
    }

    static SearchTypes.SearchQuery rangeLong(IndexKey field,
                                             long min,
                                             boolean minInclusive,
                                             long max,
                                             boolean maxInclusive) {
        return rangeLong(field.getIndexFieldName(), min, minInclusive, max, maxInclusive);
    }

    static SearchTypes.SearchQuery rangeInt(String field,
                                            int min,
                                            boolean minInclusive,
                                            int max,
                                            boolean maxInclusive) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.RANGE_INT)
                .setRangeInt(SearchTypes.SearchQuery.RangeInt.newBuilder().setField(field)
                        .setMin(min).setMinInclusive(minInclusive).setMax(max).setMaxInclusive(maxInclusive)).build();
    }

    static SearchTypes.SearchQuery rangeInt(IndexKey field,
                                            int min,
                                            boolean minInclusive,
                                            int max,
                                            boolean maxInclusive) {
        return rangeInt(field.getIndexFieldName(), min, minInclusive, max, maxInclusive);
    }

    static SearchTypes.SearchQuery rangeDouble(String field,
                                               double min,
                                               boolean minInclusive,
                                               double max,
                                               boolean maxInclusive) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.RANGE_DOUBLE)
                .setRangeDouble(SearchTypes.SearchQuery.RangeDouble.newBuilder().setField(field)
                        .setMin(min).setMinInclusive(minInclusive).setMax(max).setMaxInclusive(maxInclusive)).build();
    }

    static SearchTypes.SearchQuery rangeDouble(IndexKey field,
                                               double min,
                                               boolean minInclusive,
                                               double max,
                                               boolean maxInclusive) {
        return rangeDouble(field.getIndexFieldName(), min, minInclusive, max, maxInclusive);
    }

    static SearchTypes.SearchQuery rangeTerm(String field,
                                             String min,
                                             boolean minInclusive,
                                             String max,
                                             boolean maxInclusive) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.RANGE_TERM)
                .setRangeTerm(SearchTypes.SearchQuery.RangeTerm.newBuilder().setField(field)
                        .setMin(min).setMinInclusive(minInclusive).setMax(max).setMaxInclusive(maxInclusive)).build();
    }

    static SearchTypes.SearchQuery rangeTerm(IndexKey field,
                                             String min,
                                             boolean minInclusive,
                                             String max,
                                             boolean maxInclusive) {
        return rangeTerm(field.getIndexFieldName(), min, minInclusive, max, maxInclusive);
    }

    static SearchTypes.SearchQuery wildcard(String field, String value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.WILDCARD)
                .setWildcard(SearchTypes.SearchQuery.Wildcard.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery wildcard(IndexKey field, String value) {
        return wildcard(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery termInt(String field, int value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.TERM_INT)
                .setTermInt(SearchTypes.SearchQuery.TermInt.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery termInt(IndexKey field, int value) {
        return termInt(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery termLong(String field, long value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.TERM_LONG)
                .setTermLong(SearchTypes.SearchQuery.TermLong.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery termLong(IndexKey field, long value) {
        return termLong(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery termDouble(String field, double value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.TERM_DOUBLE)
                .setTermDouble(SearchTypes.SearchQuery.TermDouble.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery termDouble(IndexKey field, double value) {
        return termDouble(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery exists(String field) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.EXISTS)
                .setExists(SearchTypes.SearchQuery.Exists.newBuilder().setField(field)).build();
    }

    static SearchTypes.SearchQuery exists(IndexKey field) {
        return exists(field.getIndexFieldName());
    }

    static SearchTypes.SearchQuery doesNotExists(String field) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.DOES_NOT_EXIST)
                .setExists(SearchTypes.SearchQuery.Exists.newBuilder().setField(field)).build();
    }

    static SearchTypes.SearchQuery doesNotExists(IndexKey field) {
        return doesNotExists(field.getIndexFieldName());
    }

    static SearchTypes.SearchQuery boost(float boost, SearchTypes.SearchQuery searchQuery) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.BOOST)
                .setBoost(SearchTypes.SearchQuery.Boost.newBuilder().setBoost(boost).setClause(searchQuery)).build();
    }

    static SearchTypes.SearchQuery contains(String field, String value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.CONTAINS)
                .setContainsText(SearchTypes.SearchQuery.Contains.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery contains(IndexKey field, String value) {
        return contains(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery prefix(String field, String value) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.PREFIX)
                .setPrefix(SearchTypes.SearchQuery.Prefix.newBuilder().setField(field).setValue(value)).build();
    }

    static SearchTypes.SearchQuery prefix(IndexKey field, String value) {
        return prefix(field.getIndexFieldName(), value);
    }

    static SearchTypes.SearchQuery in(IndexKey field, List<String> values) {
        return in(field.getIndexFieldName(), values);
    }

    static SearchTypes.SearchQuery in(String field, List<String> values) {
        return SearchTypes.SearchQuery.newBuilder().setType(SearchTypes.SearchQuery.Type.IN)
                .setIn(SearchTypes.SearchQuery.In.newBuilder().setField(field).addAllValue(values)).build();
    }
}
