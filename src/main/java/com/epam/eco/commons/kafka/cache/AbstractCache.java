/*
 * Copyright 2019 EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.eco.commons.kafka.cache;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrei_Tytsik
 */
abstract class AbstractCache<K, V> implements Cache<K, V> {

    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractCache.class);

    private final boolean storeData;
    private final FireUpdateMode fireUpdateMode;
    private final CacheListener<K, V> listener;

    private final Map<K, V> cache = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    AbstractCache(
            boolean storeData,
            FireUpdateMode fireUpdateMode,
            CacheListener<K, V> listener) {
        Validate.notNull(fireUpdateMode, "FireUpdateMode is null");

        this.storeData = storeData;
        this.fireUpdateMode = fireUpdateMode;
        this.listener = listener;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public V get(K key) {
        lock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<K, V> getAll(Collection<K> keys) {
        Validate.notNull(keys, "Collection of keys is null");

        lock.readLock().lock();
        try {
            return keys.stream().
                    filter(k -> cache.containsKey(k)).
                    collect(
                            Collectors.toMap(Function.identity(), k -> cache.get(k)));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            return cache.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<K> keysAsSet() {
        lock.readLock().lock();
        try {
            return cache.keySet().stream().
                    collect(Collectors.toSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<K> keysAsList() {
        lock.readLock().lock();
        try {
            return cache.keySet().stream().
                    collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(V value) {
        lock.readLock().lock();
        try {
            return cache.containsValue(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<V> values() {
        lock.readLock().lock();
        try {
            return cache.values().stream().
                    collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<V> values(Collection<K> keys) {
        Validate.notNull(keys, "Collection of keys is null");

        lock.readLock().lock();
        try {
            return keys.stream().
                    filter(k -> cache.containsKey(k)).
                    map(k -> cache.get(k)).
                    collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<V> values(Predicate<V> filter) {
        Validate.notNull(filter, "Filter is null");

        lock.readLock().lock();
        try {
            return cache.values().stream().
                    filter(filter).
                    collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void remove(K key) {
        putAll(Collections.singletonMap(key, null));
    }

    @Override
    public void removeAll(Collection<K> keys) {
        Validate.notNull(keys, "Collection of keys is null");

        Map<K, V> update = keys.stream().
                collect(HashMap::new, (map, k) -> map.put(k, null), HashMap::putAll);
        putAll(update);
    }

    @Override
    public void put(K key, V value) {
        putAll(Collections.singletonMap(key, value));
    }

    @Override
    public void putAll(Map<K, V> update) {
        Validate.notEmpty(update, "Update is null or empty");

        lock.writeLock().lock();
        try {
            applyUpdateToUnderlying(update);

            if (storeData) {
                update.entrySet().forEach(entry -> {
                    K key = entry.getKey();
                    V value = entry.getValue();
                    if (value != null) {
                        cache.put(key, value);
                    } else {
                        cache.remove(key);
                    }
                });
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
    }

    protected void applyUpdateFromUnderlying(Map<K, V> update) {
        if (update.isEmpty()) {
            return;
        }

        Map<K, V> effective = update;

        if (storeData) {
            lock.writeLock().lock();
            try {
                effective = new HashMap<>();
                for (Entry<K, V> entry : update.entrySet()) {
                    K key = entry.getKey();
                    V newValue = entry.getValue();

                    V oldValue = cache.get(key);
                    if (!Objects.equals(oldValue, newValue)) {
                        if (newValue != null) {
                            cache.put(key, newValue);
                        } else {
                            cache.remove(key);
                        }
                        effective.put(key, newValue);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        if (FireUpdateMode.ALL == fireUpdateMode) {
            fireCacheListener(update);
        } else if (FireUpdateMode.EFFECTIVE == fireUpdateMode) {
            fireCacheListener(effective);
        } else {
            throw new RuntimeException(
                    String.format("Fire update mode '%s' is not supported", fireUpdateMode));
        }
    }

    protected void applyUpdateToUnderlying(Map<K, V> update) {
        throw new UnsupportedOperationException();
    }

    private void fireCacheListener(Map<K, V> update) {
        if (listener == null || update.isEmpty()) {
            return;
        }

        try {
            listener.onCacheUpdated(update);
        } catch (Exception ex) {
            LOGGER.error("Listener failed while handling cache update", ex);
        }
    }

}
