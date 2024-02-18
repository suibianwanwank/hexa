package com.ccsu.store.rocksDB;

import com.ccsu.store.api.Converter;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.FindByRange;
import com.ccsu.store.api.Format;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.store.api.StoreConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Deprecated
public class RocksDBStore<K, V> implements DataStore<K, V> {
    private RocksDB rocksDB;
    private final ColumnFamilyDescriptor family;
    private final ColumnFamilyHandle handle;
    private final String name;
    private final int parallel;
    private final Converter<K, byte[]> keyBytesConverter;
    private final Format<V> valueFormat;

    public RocksDBStore(RocksDB rocksDB,
                        ColumnFamilyDescriptor family,
                        ColumnFamilyHandle handle,
                        String name,
                        int parallel,
                        Converter<K, byte[]> keyBytesConverter,
                        Format<V> valueFormat) {
        this.rocksDB = rocksDB;
        this.family = family;
        this.handle = handle;
        this.name = name;
        this.parallel = parallel;
        this.keyBytesConverter = keyBytesConverter;
        this.valueFormat = valueFormat;
    }

    @Nullable
    @Override
    public EntityWithTag<K, V> get(K key) {
        byte[] value = null;
        try {
            value = rocksDB.get(handle, keyBytesConverter.convert(key));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return value == null ? null : toEntity(key, value);
    }

    private EntityWithTag<K, V> toEntity(K key, byte[] value) {
        return new EntityWithTag<K, V>() {
            @Override
            public K getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return valueFormat.getBytesConverter().revert(value);
            }
        };
    }

    @Override
    public Iterable<? extends EntityWithTag<K, V>> get(List<K> keys) {
        return null;
    }

    @Override
    public boolean contains(K key) {
        return false;
    }

    @Override
    public void put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        try {
            rocksDB.put(handle, keyBytesConverter.convert(key),
                    requireNonNull(valueFormat.getBytesConverter()).convert(value));
        } catch (RocksDBException e) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, e.getMessage());
        }
    }

    @Override
    public void delete(K key) {

    }

    @Override
    public Iterable<? extends EntityWithTag<K, V>> find() {
        return null;
    }

    @Override
    public Iterable<? extends EntityWithTag<K, V>> find(FindByRange<K> findByRange) {
        return null;
    }

    @Override
    public String validateTagThenPut(K key, V value, @Nullable String currentTag) {
        return null;
    }

    @Override
    public void validateTagThenDelete(K key, String currentTag) {

    }

    @Override
    public StoreConfig<K, V> getStoreConfig() {
        return null;
    }


    @Override
    public void close() throws Exception {

    }
}
