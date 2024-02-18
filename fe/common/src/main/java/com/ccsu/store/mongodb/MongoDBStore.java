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

import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.FindByCondition;
import com.ccsu.store.api.FindByRange;
import com.ccsu.store.api.ImmutableEntityWithTag;
import com.ccsu.store.api.StoreConfig;
import com.facebook.airlift.log.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.QueryOperators;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nullable;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * use rocksdb store kv data
 */
public class MongoDBStore
        implements DataIndexStore<String, Bson> {
    private static final Logger LOGGER = Logger.get(MongoDBStore.class);

    private final MongoCollection<Document> collection;
    private final StoreConfig<?, ?> storeConfig;

    public MongoDBStore(MongoDatabase mongoDatabase,
                        final StoreConfig<?, ?> storeConfig) {
        requireNonNull(storeConfig, "storeConfig is empty");
        this.collection = mongoDatabase.getCollection(storeConfig.name());
        this.storeConfig = storeConfig;
        MongoIndexConvertUtil.initIndex(collection, storeConfig.indexConverter());
    }

    public MongoCollection<Document> getCollection() {
        return collection;
    }

    @Override
    @Nullable
    public EntityWithTag<String, Bson> get(String key) {
        Document document = this.collection.find(Filters.eq("_id", convertId(key)))
                .limit(1)
                .first();
        if (document == null) {
            return null;
        }
        return toEntity(key, document);
    }

    // ? multi-get
    @Override
    public Iterable<EntityWithTag<String, Bson>> get(List<String> keys) {
        requireNonNull(keys, "keys is empty");
        FindIterable<Document> documents = this.collection.find(Filters.in("_id", keys));
        return StreamSupport.stream(documents.spliterator(), false).map(document -> {
            String id = document.getString("_id");
            String key = revertId(id);
            return toEntity(key, document);
        }).collect(Collectors.toList());
    }

    @Override
    public boolean contains(String key) {
        Document document = this.collection.find(Filters.eq("_id", convertId(key)))
                .limit(1)
                .first();
        return document != null;
    }

    @Override
    public void put(String key, Bson value) {
        requireNonNull(key, "key is empty");
        requireNonNull(value, "value is empty");
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        UpdateResult updateResult =
                collection.updateOne(Filters.eq("_id", convertId(key)),
                        new Document("$set", value),
                        updateOptions);
        LOGGER.debug("update result:%s", updateResult);
    }

    @Override
    public void delete(String key) {
        this.collection.deleteOne(Filters.eq("_id", convertId(key)));
    }

    @Override
    public Iterable<EntityWithTag<String, Bson>> find() {
        FindIterable<Document> documents = this.collection.find();
        return StreamSupport.stream(documents.spliterator(), false).map(document -> {
            String id = document.getString("_id");
            String key = revertId(id);
            return toEntity(key, document);
        }).collect(Collectors.toList());
    }

    @Override
    public Iterable<? extends EntityWithTag<String, Bson>> find(FindByRange<String> findByRange) {
        requireNonNull(findByRange, "findByRange is null");
        BasicDBObject query = new BasicDBObject();
        if (findByRange.getStart() != null) {
            query.append(findByRange.isStartInclusive()
                    ? QueryOperators.GTE : QueryOperators.GT, findByRange.getStart());
        }
        if (findByRange.getEnd() != null) {
            query.append(findByRange.isEndInclusive()
                    ? QueryOperators.LTE : QueryOperators.LT, findByRange.getEnd());
        }
        FindIterable<Document> documents = this.collection.find(new BasicDBObject("_id", query));
        return StreamSupport.stream(documents.spliterator(), false).map(document -> {
            String id = document.getString("_id");
            String key = revertId(id);
            return toEntity(key, document);
        }).collect(Collectors.toList());
    }

    @Override
    public String validateTagThenPut(String key, Bson value, @Nullable String currentTag) {
        requireNonNull(key);
        requireNonNull(value);
        BsonDocument document = value.toBsonDocument(value.getClass(), collection.getCodecRegistry());
        String newTag = UUID.randomUUID().toString();
        document.put("tag", new BsonString(newTag));
        BasicDBObject query = new BasicDBObject();
        query.put("_id", convertId(key));
        try {
            UpdateOptions updateOptions = new UpdateOptions();
            if (currentTag == null || "".equals(currentTag)) {
                query.put("tag", null);
            } else {
                query.put("tag", currentTag);
            }
            updateOptions.upsert(true);
            UpdateResult updateResult = collection.updateOne(query, new Document("$set", document), updateOptions);
            LOGGER.debug("update result:%s", updateResult);
            return newTag;
        } catch (MongoWriteException | DuplicateKeyException e) {
            Document oldDoc = this.collection.find(Filters.eq("_id", convertId(key)))
                    .limit(1)
                    .first();
            if (oldDoc == null) {
                throw new ConcurrentModificationException("update failed, document[" + key + "] not exist");
            }
            throw new ConcurrentModificationException("update failed key=" + key + " now tag is "
                    + oldDoc.getString("tag") + " and version control is " + currentTag);
        }
    }

    @Override
    public void validateTagThenDelete(String key, String currentTag) {
        requireNonNull(key);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", key);
        query.put("tag", currentTag);
        this.collection.deleteOne(query);
    }

    @Override
    public StoreConfig getStoreConfig() {
        return storeConfig;
    }

    @Override
    public Iterable<? extends EntityWithTag<String, Bson>> find(FindByCondition findByCondition) {
        FindIterable<Document> documents;
        if (findByCondition.getCondition() != null) {
            Bson query = MongoSearchConverter.INSTANCE.toQuery(findByCondition.getCondition());
            documents = this.collection.find(query);
        } else {
            documents = this.collection.find();
        }
        documents = documents.sort(MongoSearchConverter.INSTANCE.toSort(findByCondition.getSort()))
                .skip(findByCondition.getOffset())
                .limit(Math.min(findByCondition.getPageSize(), findByCondition.getLimit()));
        return StreamSupport.stream(documents.spliterator(), false).map(document -> {
            String id = document.getString("_id");
            String key = revertId(id);
            return toEntity(key, document);
        }).collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
    }

    private String convertId(String key) {
        return key;
    }

    private String revertId(String id) {
        return id;
    }

    private EntityWithTag<String, Bson> toEntity(String key, Document document) {
        return ImmutableEntityWithTag.<String, Bson>builder().key(key).tag(document.getString("tag"))
                .value(document).build();
    }
}
