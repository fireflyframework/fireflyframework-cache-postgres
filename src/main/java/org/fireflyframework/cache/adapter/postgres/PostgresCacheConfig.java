/*
 * Copyright 2024-2026 Firefly Software Foundation
 *
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

package org.fireflyframework.cache.adapter.postgres;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;

/**
 * Configuration for the PostgreSQL R2DBC cache adapter.
 * <p>
 * Holds the cache identity (name, key prefix), the default time-to-live applied
 * to entries when none is supplied explicitly, and the physical storage location
 * (schema and table) for cache rows.
 */
@Data
@Builder
public class PostgresCacheConfig {

    /**
     * Logical cache name. Stored in the {@code cache_name} column so that a single
     * table can host multiple named caches.
     */
    private final String cacheName;

    /**
     * Key prefix applied to every cache key. Used to namespace keys within a cache.
     */
    private final String keyPrefix;

    /**
     * Default time-to-live applied when no explicit TTL is provided on a put.
     */
    @Builder.Default
    private final Duration defaultTtl = Duration.ofMinutes(30);

    /**
     * PostgreSQL schema that holds the cache table.
     */
    @Builder.Default
    private final String schema = "public";

    /**
     * Name of the table that stores cache entries.
     */
    @Builder.Default
    private final String cacheTable = "firefly_cache_entries";
}
