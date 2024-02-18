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
package com.ccsu.store.mongodb;

import com.ccsu.datastore.api.SearchTypes;
import com.facebook.airlift.log.Logger;
import com.google.common.base.CharMatcher;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryOperators;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class MongoSearchConverter {
    public static final char WILDCARD_STRING = '*';

    public static final char WILDCARD_CHAR = '?';

    public static final char WILDCARD_ESCAPE = '\\';

    private static final Logger LOGGER = Logger.get(MongoSearchConverter.class);

    public static final MongoSearchConverter INSTANCE = new MongoSearchConverter();

    private final CharMatcher specialCharactersMatcher = CharMatcher.anyOf(new String(new char[]{
            WILDCARD_ESCAPE, WILDCARD_CHAR, WILDCARD_STRING
    })).precomputed();

    private MongoSearchConverter() {
    }

    Bson toQuery(SearchTypes.SearchQuery query) {
        switch (query.getType()) {
            case BOOLEAN:
                return toBooleanQuery(query.getBoolean());

            case MATCH_ALL:
                return toMatchAllQuery(query.getMatchAll());

            case RANGE_DOUBLE:
                return toRangeQuery(query.getRangeDouble());

            case RANGE_INT:
                return toRangeQuery(query.getRangeInt());

            case RANGE_LONG:
                return toRangeQuery(query.getRangeLong());

            case RANGE_TERM:
                return toRangeQuery(query.getRangeTerm());

            case TERM:
                return toTermQuery(query.getTerm());

            case WILDCARD:
                return toWildcardQuery(query.getWildcard());

            case TERM_INT:
                return toTermIntQuery(query.getTermInt());

            case TERM_LONG:
                return toTermLongQuery(query.getTermLong());

            case TERM_DOUBLE:
                return toTermDoubleQuery(query.getTermDouble());

            case EXISTS:
                return toExistsQuery(query.getExists());

            case DOES_NOT_EXIST:
                return toDoesNotExistQuery(query.getExists());

            case BOOST:
                return toBoostQuery(query.getBoost());

            case CONTAINS:
                return toContainsTermQuery(query.getContainsText());

            case PREFIX:
                return toPrefixQuery(query.getPrefix());

            case IN:
                return toInQuery(query.getIn());

            case NOT_EQUAL:
                return toNotEqual(query.getNotEqual());

            default:
                throw new AssertionError("Unknown query type: " + query);
        }
    }

    private StringBuilder escapeTextForWildcard(String text) {
        final StringBuilder sb = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char currentChar = text.charAt(i);
            if (specialCharactersMatcher.matches(currentChar)) {
                sb.append(WILDCARD_ESCAPE);
            }
            sb.append(currentChar);
        }
        return sb;
    }

    private Bson toBooleanQuery(SearchTypes.SearchQuery.Boolean booleanQuery) {
        List<Bson> clauses = new ArrayList<>();
        for (SearchTypes.SearchQuery clause : booleanQuery.getClausesList()) {
            clauses.add(toQuery(clause));
        }
        switch (booleanQuery.getOp()) {
            case AND:
                return Filters.and(clauses);
            case OR:
                return Filters.or(clauses);
            default:
                throw new AssertionError("Unknown boolean operator: " + booleanQuery.getOp());
        }
    }

    private Bson toMatchAllQuery(SearchTypes.SearchQuery.MatchAll matchAll) {
        return new BasicDBObject();
    }

    private Bson toRangeQuery(SearchTypes.SearchQuery.RangeDouble range) {
        return toRangeQuery(range.getField(),
                range.hasMin(),
                range.getMinInclusive(),
                range.getMin(),
                range.hasMax(),
                range.getMaxInclusive(),
                range.getMax());
    }

    private Bson toRangeQuery(SearchTypes.SearchQuery.RangeInt range) {
        return toRangeQuery(range.getField(),
                range.hasMin(),
                range.getMinInclusive(),
                range.getMin(),
                range.hasMax(),
                range.getMaxInclusive(),
                range.getMax());
    }

    private Bson toRangeQuery(SearchTypes.SearchQuery.RangeLong range) {
        return toRangeQuery(range.getField(),
                range.hasMin(),
                range.getMinInclusive(),
                range.getMin(),
                range.hasMax(),
                range.getMaxInclusive(),
                range.getMax());
    }

    private Bson toRangeQuery(SearchTypes.SearchQuery.RangeTerm range) {
        return toRangeQuery(range.getField(),
                range.hasMin(),
                range.getMinInclusive(),
                range.getMin(),
                range.hasMax(),
                range.getMaxInclusive(),
                range.getMax());
    }

    private Bson toRangeQuery(String field,
                              boolean hasMin,
                              boolean minInclusive,
                              Comparable<?> min,
                              boolean hasMax,
                              boolean maxInclusive,
                              Comparable<?> max) {
        BasicDBObject query = new BasicDBObject();
        if (hasMin) {
            query.put(minInclusive ? QueryOperators.GTE : QueryOperators.GT, min);
        }
        if (hasMax) {
            query.put(maxInclusive ? QueryOperators.LTE : QueryOperators.LT, max);
        }
        return new BasicDBObject(MongoIndexConvertUtil.formatIndexFieldName(field), query);
    }

    private Bson toNotEqual(SearchTypes.SearchQuery.NotEqual notEqual) {
        return Filters.ne(MongoIndexConvertUtil.formatIndexFieldName(notEqual.getField()), notEqual.getValue());
    }

    private Bson toTermQuery(SearchTypes.SearchQuery.Term term) {
        return Filters.eq(MongoIndexConvertUtil.formatIndexFieldName(term.getField()), term.getValue());
    }

    private Bson toWildcardQuery(SearchTypes.SearchQuery.Wildcard wildcard) {
        return Filters.regex(MongoIndexConvertUtil.formatIndexFieldName(wildcard.getField()), wildcard.getValue());
    }

    private Bson toPrefixQuery(SearchTypes.SearchQuery.Prefix prefix) {
        String formatName = MongoIndexConvertUtil.formatIndexFieldName(prefix.getField());
        return Filters.regex(formatName, String.format("^%s", prefix.getValue()));
    }

    private Bson toContainsTermQuery(SearchTypes.SearchQuery.Contains containsQuery) {
        final StringBuilder sb = escapeTextForWildcard(containsQuery.getValue());
        return Filters.regex(MongoIndexConvertUtil.formatIndexFieldName(containsQuery.getField()), sb.toString());
    }

    private Bson toTermIntQuery(SearchTypes.SearchQuery.TermInt term) {
        return Filters.eq(MongoIndexConvertUtil.formatIndexFieldName(term.getField()), term.getValue());
    }

    private Bson toTermLongQuery(SearchTypes.SearchQuery.TermLong term) {
        return Filters.eq(MongoIndexConvertUtil.formatIndexFieldName(term.getField()), term.getValue());
    }

    private Bson toTermDoubleQuery(SearchTypes.SearchQuery.TermDouble term) {
        return Filters.eq(MongoIndexConvertUtil.formatIndexFieldName(term.getField()), term.getValue());
    }

    private Bson toExistsQuery(SearchTypes.SearchQuery.Exists exists) {
        return Filters.exists(MongoIndexConvertUtil.formatIndexFieldName(exists.getField()));
    }

    private Bson toDoesNotExistQuery(SearchTypes.SearchQuery.Exists exists) {
        return Filters.exists(MongoIndexConvertUtil.formatIndexFieldName(exists.getField()), false);
    }

    private Bson toInQuery(SearchTypes.SearchQuery.In in) {
        return Filters.in(MongoIndexConvertUtil.formatIndexFieldName(in.getField()), in.getValueList());
    }

    private Bson toBoostQuery(SearchTypes.SearchQuery.Boost boost) {
        LOGGER.warn("MongoDB IndexDataStore not support boost query");
        return new BasicDBObject();
    }

    public Bson toSort(List<SearchTypes.SearchFieldSorting> orderings) {
        if (orderings.isEmpty()) {
            return Sorts.orderBy();
        }
        List<Bson> sorts = new ArrayList<>();
        for (SearchTypes.SearchFieldSorting ordering : orderings) {
            sorts.add(SearchTypes.SearchFieldSorting.SortOrder.ASC == ordering.getOrder()
                    ? Sorts.ascending(MongoIndexConvertUtil.formatIndexFieldName(ordering.getField()))
                    : Sorts.descending(MongoIndexConvertUtil.formatIndexFieldName(ordering.getField())));
        }
        return Sorts.orderBy(sorts);
    }
}
