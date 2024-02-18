package com.ccsu.store.rocksDB;

import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.StoreManager;
import com.ccsu.store.api.Converter;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.Format;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.store.mongodb.StoreMetadataCommitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Deprecated
public class RocksDBManager implements StoreManager {
    public static final long FILTER_SIZE_IN_BYTES = 1024 * 1024;
    private static final long DEFAULT_SIZE_LIMIT = 60L;
    private RocksDB rocksDB;
    private ColumnFamilyHandle defaultHandle;

    private final ConcurrentMap<Integer, String> handleIdToNameMap = new ConcurrentHashMap<>();

    private final Cache<String, DataStore> rocksDBStoreMaps = CacheBuilder.newBuilder()
            .removalListener((RemovalListener<String, DataStore>) notification -> {
                try {
                    notification.getValue().close();
                } catch (Exception e) {
                    throw new CommonException(CommonErrorCode.METADATA_ERROR, e.getMessage());
                }
            }).build();

    public RocksDBManager(File file) {
        init(file);
    }

    private void init(File dbDirectory) {
        if (dbDirectory.exists() && !dbDirectory.isDirectory()) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR,
                    String.format("Invalid path %s for local catalog db, not a directory.",
                            dbDirectory.getAbsolutePath()));
        } else if (!dbDirectory.exists()) {
            if (!dbDirectory.mkdirs()) {
                throw new CommonException(CommonErrorCode.METADATA_ERROR, "can't make dir");
            }
        }

        final String path = dbDirectory.toString();

        final List<byte[]> families;
        try (final Options options = new Options()) {
            options.setCreateIfMissing(true);
            // get a list of existing families.
            try {
                families = new ArrayList<>(RocksDB.listColumnFamilies(options, path));
            } catch (RocksDBException e) {
                throw new CommonException(CommonErrorCode.METADATA_ERROR, e.getMessage());
            }
        }

        if (families.isEmpty()) {
            families.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        final Function<byte[], ColumnFamilyDescriptor> function = ColumnFamilyDescriptor::new;

        List<ColumnFamilyHandle> familyHandles = new ArrayList<>();

        try (final DBOptions dboptions = new DBOptions()) {
            dboptions.setCreateIfMissing(true);
            dboptions.setWalSizeLimitMB(0);
            dboptions.setWalTtlSeconds(5 * DEFAULT_SIZE_LIMIT);
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = families.stream()
                    .map(ColumnFamilyDescriptor::new)
                    .collect(Collectors.toList());
            this.rocksDB =
                    RocksDB.open(dboptions, path, columnFamilyDescriptorList, familyHandles);
        } catch (RocksDBException e) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, e.getMessage());
        }

        for (int i = 0; i < families.size(); i++) {
            byte[] family = families.get(i);
            if (Arrays.equals(family, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                defaultHandle = familyHandles.get(i);
            } else {
                String name = new String(family, UTF_8);
                final ColumnFamilyHandle handle = familyHandles.get(i);
                handleIdToNameMap.put(handle.getID(), name);
                ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(family);
                RocksDBStore<Object, Object> store =
                        newRocksDBStore(name, descriptor, handle, null, null);
                rocksDBStoreMaps.put(name, store);
            }
        }

    }

    private <K, V> DataStore<K, V> newStore(String name, Converter<K, byte[]> converter, Format<V> format) {
        try {
            final ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name.getBytes(UTF_8));
            ColumnFamilyHandle handle = rocksDB.createColumnFamily(columnFamilyDescriptor);
            return newRocksDBStore(name, columnFamilyDescriptor, handle, converter, format);
        } catch (RocksDBException e) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, e.getMessage());
        }
    }

    private <K, V> RocksDBStore<K, V> newRocksDBStore(String name,
                                                      ColumnFamilyDescriptor columnFamilyDescriptor,
                                                      ColumnFamilyHandle handle,
                                                      Converter<K, byte[]> converter, Format<V> format) {
        return new RocksDBStore<K, V>(rocksDB, columnFamilyDescriptor, handle, name, 1, converter, format);
    }

    @Override
    public void start() {

    }

    @Override
    public <K, V> DataStore<K, V> getOrCreateDataStore(StoreConfig<K, V> storeConfig) {
        requireNonNull(storeConfig);
        requireNonNull(storeConfig.name());
        try {
            DataStore<K, V> rocksDBStore = rocksDBStoreMaps.get(
                    storeConfig.name(),
                    () -> newStore(storeConfig.name(),
                            storeConfig.keyBytesConverter(),
                            storeConfig.valueFormat()));
            return rocksDBStore;
        } catch (ExecutionException e) {
            throw new CommonException(CommonErrorCode.METADATA_ERROR, e.getMessage());
        }
    }

    @Override
    public <K, V> DataIndexStore<K, V> getOrCreateDataIndexStore(StoreConfig<K, V> storeConfig) {
        return null;
    }

    @Nullable
    @Override
    public DataStore getDataStore(String name) {
        requireNonNull(name);
        return rocksDBStoreMaps.getIfPresent(name);
    }

    @Nullable
    @Override
    public <K, V> DataIndexStore<K, V> getDataIndexStore(String name) {
        return null;
    }

    @Override
    public StoreMetadataCommitter getStoreMetaCommitter() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
