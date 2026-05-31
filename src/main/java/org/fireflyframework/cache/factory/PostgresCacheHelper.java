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

package org.fireflyframework.cache.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.spi.ConnectionFactory;
import org.fireflyframework.cache.adapter.postgres.PostgresCacheAdapter;
import org.fireflyframework.cache.adapter.postgres.PostgresCacheConfig;
import org.fireflyframework.cache.core.CacheAdapter;
import org.fireflyframework.cache.properties.CacheProperties;
import org.fireflyframework.cache.serialization.CacheSerializer;
import org.fireflyframework.cache.serialization.JsonCacheSerializer;

import java.time.Duration;

/**
 * Self-contained factory helper that builds a {@link PostgresCacheAdapter}.
 * <p>
 * The fully-qualified class name {@code org.fireflyframework.cache.factory.PostgresCacheHelper}
 * is referenced reflectively by the cache core, so it must not be renamed or
 * moved to a different package.
 */
public final class PostgresCacheHelper {

    private PostgresCacheHelper() {
    }

    /**
     * Creates a PostgreSQL cache adapter from the supplied connection factory and
     * cache properties.
     *
     * @param cacheName              the logical cache name
     * @param keyPrefix              the key prefix applied to all cache keys
     * @param defaultTtl             default TTL applied when no explicit TTL is supplied
     * @param r2dbcConnectionFactory the R2DBC {@code ConnectionFactory} (typed as Object so the
     *                               cache core never compiles against R2DBC types)
     * @param properties             the cache properties (Postgres config is read from {@code getPostgres()})
     * @param objectMapper           the Jackson object mapper used for value serialization
     * @return a configured {@link CacheAdapter} backed by PostgreSQL
     */
    public static CacheAdapter createPostgresCacheAdapter(String cacheName,
                                                          String keyPrefix,
                                                          Duration defaultTtl,
                                                          Object r2dbcConnectionFactory,
                                                          CacheProperties properties,
                                                          ObjectMapper objectMapper) {
        CacheSerializer serializer = new JsonCacheSerializer(objectMapper);
        CacheProperties.PostgresConfig postgres = properties.getPostgres();

        Duration ttl = defaultTtl != null
                ? defaultTtl
                : (postgres.getDefaultTtl() != null ? postgres.getDefaultTtl() : Duration.ofMinutes(30));
        String prefix = keyPrefix != null ? keyPrefix : postgres.getKeyPrefix();

        PostgresCacheConfig config = PostgresCacheConfig.builder()
                .cacheName(cacheName)
                .keyPrefix(prefix)
                .defaultTtl(ttl)
                .schema(postgres.getSchema() != null ? postgres.getSchema() : "public")
                .cacheTable(postgres.getCacheTable() != null ? postgres.getCacheTable() : "firefly_cache_entries")
                .build();

        ConnectionFactory connectionFactory = (ConnectionFactory) r2dbcConnectionFactory;
        return new PostgresCacheAdapter(cacheName, connectionFactory, config, serializer);
    }
}
