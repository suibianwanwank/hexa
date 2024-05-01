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

import com.facebook.airlift.configuration.Config;

public class MongoDBStoreManagerConfig {
    private String uri;

    private String database;

    @Config("datastore.mongodb.uri")
    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUri() {
        return uri;
    }

    @Config("datastore.mongodb.db")
    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }
}
