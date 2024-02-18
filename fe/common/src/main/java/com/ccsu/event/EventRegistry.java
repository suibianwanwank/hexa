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
package com.ccsu.event;

/**
 * Registry of events.
 */
public interface EventRegistry
        extends AutoCloseable {
    /**
     * start event registry
     */
    void start();

    /**
     * add listener to event registry
     *
     * @param name     event name
     * @param listener event listener
     */
    void addListener(String name, EventListener listener);

    /**
     * remove listener to event registry
     *
     * @param name     event name
     * @param listener event listener
     */
    void remove(String name, EventListener listener);

    /**
     * event publish for this event register
     *
     * @return event publisher
     */
    EventPublisher getEventPublisher();
}
