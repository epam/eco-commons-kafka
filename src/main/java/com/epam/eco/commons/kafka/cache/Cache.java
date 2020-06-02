/*
 * Copyright 2020 EPAM Systems
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

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * @author Andrei_Tytsik
 */
public interface Cache<K, V> extends Closeable {
    void start() throws Exception;
    V get(K key);
    Map<K, V> getAll(Collection<K> keys);
    boolean containsKey(K key);
    Set<K> keysAsSet();
    List<K> keysAsList();
    boolean containsValue(V value);
    List<V> values();
    List<V> values(Collection<K> keys);
    List<V> values(Predicate<V> filter);
    void remove(K key);
    void removeAll(Collection<K> keys);
    void put(K key, V value);
    void putAll(Map<K, V> update);
    int size();
}
