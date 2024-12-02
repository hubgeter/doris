// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

/**
 * Factory to create Caffeine cache.
 * <p>
 * This class is used to create Caffeine cache with specified parameters.
 * It is used to create both sync and async cache.
 * The cache is created with the following parameters:
 * - expireAfterWriteSec: The duration after which the cache entries will expire.
 * - refreshAfterWriteSec: The duration after which the cache entries will be refreshed.
 * - maxSize: The maximum size of the cache.
 * - enableStats: Whether to enable stats for the cache.
 * - ticker: The ticker to use for the cache.
 * The cache can be created with the above parameters using the buildCache and buildAsyncCache methods.
 * </p>
 */
public class CacheFactory<K, V> {

    private OptionalLong expireAfterWriteSec;
    private OptionalLong refreshAfterWriteSec;
    private OptionalLong maxSize;
    private boolean enableStats;
    // Ticker is used to provide a time source for the cache.
    // Only used for test, to provide a fake time source.
    // If not provided, the system time is used.
    private Ticker ticker;

    private OptionalLong maxWeight;

    private Weigher<K, V> weigher;

    public CacheFactory(
            OptionalLong expireAfterWriteSec,
            OptionalLong refreshAfterWriteSec,
            long maxSize,
            boolean enableStats,
            Ticker ticker) {
        this(expireAfterWriteSec, refreshAfterWriteSec, OptionalLong.of(maxSize), enableStats, ticker,
                OptionalLong.empty(), null);
    }

    public CacheFactory(
            OptionalLong expireAfterWriteSec,
            OptionalLong refreshAfterWriteSec,
            boolean enableStats,
            Ticker ticker,
            long maxWeight,
            Weigher<K, V> weigher) {
        this(expireAfterWriteSec, refreshAfterWriteSec, OptionalLong.empty(), enableStats, ticker,
                OptionalLong.of(maxWeight), weigher);
    }

    private CacheFactory(
            OptionalLong expireAfterWriteSec,
            OptionalLong refreshAfterWriteSec,
            OptionalLong maxSize,
            boolean enableStats,
            Ticker ticker,
            OptionalLong maxWeight,
            Weigher<K, V> weigher) {
        this.expireAfterWriteSec = expireAfterWriteSec;
        this.refreshAfterWriteSec = refreshAfterWriteSec;
        this.maxSize = maxSize;
        this.enableStats = enableStats;
        this.ticker = ticker;
        this.maxWeight = maxWeight;
        this.weigher = weigher;
    }

    // Build a loading cache, without executor, it will use fork-join pool for refresh
    public <K, V> LoadingCache<K, V> buildCache(CacheLoader<K, V> cacheLoader) {
        Caffeine<Object, Object> builder = buildWithParams();
        return builder.build(cacheLoader);
    }

    // Build a loading cache, with executor, it will use given executor for refresh
    public <K, V> LoadingCache<K, V> buildCache(CacheLoader<K, V> cacheLoader,
            RemovalListener<K, V> removalListener, ExecutorService executor) {
        Caffeine<Object, Object> builder = buildWithParams();
        builder.executor(executor);
        if (removalListener != null) {
            builder.removalListener(removalListener);
        }
        return builder.build(cacheLoader);
    }

    public <K, V> Cache<K, V> buildCache() {
        Caffeine<Object, Object> builder = buildWithParams();
        return builder.build();
    }

    // Build an async loading cache
    public <K, V> AsyncLoadingCache<K, V> buildAsyncCache(AsyncCacheLoader<K, V> cacheLoader,
            ExecutorService executor) {
        Caffeine<Object, Object> builder = buildWithParams();
        builder.executor(executor);
        return builder.buildAsync(cacheLoader);
    }

    @NotNull
    private <K, V> Caffeine<Object, Object> buildWithParams() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (maxSize.isPresent()) {
            builder.maximumSize(maxSize.getAsLong());
        }

        if (expireAfterWriteSec.isPresent()) {
            builder.expireAfterWrite(Duration.ofSeconds(expireAfterWriteSec.getAsLong()));
        }
        if (refreshAfterWriteSec.isPresent()) {
            builder.refreshAfterWrite(Duration.ofSeconds(refreshAfterWriteSec.getAsLong()));
        }

        if (enableStats) {
            builder.recordStats();
        }

        if (ticker != null) {
            builder.ticker(ticker);
        }

        if (maxWeight.isPresent()) {
            builder.maximumWeight(maxWeight.getAsLong());
        }

        if (weigher != null) {
            builder.weigher(weigher);
        }
        return builder;
    }
}
