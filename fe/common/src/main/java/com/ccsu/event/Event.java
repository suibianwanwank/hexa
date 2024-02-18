package com.ccsu.event;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Event<T> {
    private String name;

    private T data;

    private long createTime;

    public Event() {
    }

    public Event(String name, T data) {
        this(name, data, System.currentTimeMillis());
    }

    public Event(String name, T data, long createTime) {
        this.name = requireNonNull(name, "eventName is null");
        this.data = requireNonNull(data, "data is null");
        this.createTime = createTime;
    }

    public String getName() {
        return name;
    }


    public T getData() {
        return data;
    }


    public long getCreateTime() {
        return createTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event<?> event = (Event<?>) o;
        return createTime == event.createTime
                && Objects.equals(name, event.name)
                && Objects.equals(data, event.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, data, createTime);
    }

    @Override
    public String toString() {
        return "Event{"
                + "name=" + name
                + ", data=" + data
                + ", createTime=" + createTime
                + '}';
    }
}
