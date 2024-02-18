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

import com.ccsu.error.CommonException;
import com.ccsu.utils.ExcutorServiceUtils;
import com.facebook.airlift.log.Logger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Native Implementation of EventRegistry.
 */
public class LocalEventRegistry implements EventRegistry {
    private static final Logger LOGGER = Logger.get(EventRegistry.class);

    private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;

    private final LinkedBlockingQueue<Event<?>> eventQueue;

    private final Map<String, List<EventListener>> eventListenerMap;

    private final ExecutorService executorService;

    private volatile boolean state;

    @Inject
    public LocalEventRegistry() {
        this.eventListenerMap = new ConcurrentHashMap<>();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.executorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors() * 2,
                DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("EventRegister-%d")
                        .build());
    }

    public <T> void publish(Event<T> event) {
        eventQueue.add(event);
    }

    public <T> void publishSync(Event<T> event) throws CommonException {
        List<EventListener> eventListeners = this.eventListenerMap.get(event.getName());
        if (eventListeners == null) {
            return;
        }
        for (EventListener eventListener : eventListeners) {
            eventListener.onEvent(event);
        }
    }

    @Override
    public void addListener(String name, EventListener listener) {
        List<EventListener> eventListeners =
                this.eventListenerMap.computeIfAbsent(name, (n) -> new CopyOnWriteArrayList<>());
        eventListeners.add(listener);
    }

    @Override
    public void remove(String name, EventListener listener) {
        List<EventListener> eventListeners =
                this.eventListenerMap.computeIfAbsent(name, (n) -> new CopyOnWriteArrayList<>());
        eventListeners.remove(listener);
    }

    @Override
    public EventPublisher getEventPublisher() {
        return new EventPublisher() {
            @Override
            public <T> void publish(Event<T> event) {
                LocalEventRegistry.this.publish(event);
            }

            @Override
            public <T> void publishSync(Event<T> event) throws CommonException {
                LocalEventRegistry.this.publishSync(event);
            }
        };
    }

    @Override
    @PostConstruct
    public void start() {
        this.state = true;
        this.executorService.execute(() -> {
            while (this.state) {
                try {
                    Event<?> event = eventQueue.poll(1, TimeUnit.SECONDS);
                    if (event == null) {
                        continue;
                    }
                    List<EventListener> eventListeners = this.eventListenerMap.get(event.getName());
                    if (eventListeners == null || eventListeners.isEmpty()) {
                        continue;
                    }
                    for (EventListener eventListener : eventListeners) {
                        executorService.execute(() -> {
                            try {
                                eventListener.onEvent(event);
                            } catch (Throwable e) {
                                LOGGER.error(e, "Handle message error: %s", event);
                            }
                        });
                    }
                } catch (Throwable e) {
                    LOGGER.info("Poll message error: %s", e);
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        this.state = false;
        ExcutorServiceUtils.close(this.executorService);
    }
}
